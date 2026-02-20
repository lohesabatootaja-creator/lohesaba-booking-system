/******************************
 * Lohesaba Booking System v2.6
 ******************************/

const CONFIG = {
  TIMEZONE: 'Europe/Tallinn',

  CALENDAR_ID: 'lohesaba.tootaja@gmail.com',

  BACKUP_FOLDER_ID: '1UhqEvweWMnspopDiaKMwLnHVANaGKtT_',
  BANK_FOLDER_ID: '1GLqlpBwLtEjLdvXQLiDg-Tkyv46uPQAM',
  LOG_FOLDER_ID: '1UhqEvweWMnspopDiaKMwLnHVANaGKtT_',

  ADMIN_EMAIL: 'mjdexpert@gmail.com',

  DAYS_TO_KEEP_BACKUPS: 90,

  LOOKBACK_DAYS: 14,
  LOOKAHEAD_DAYS: 365,

  DEBT_HOURS: 48,

  CONFLICT_BUFFER_MIN: 30,
  COLOR_CONFLICT: '8',

  COLOR_DEFAULT: '7',
  COLOR_DEBTOR: '11',
  ROOM_COLORS: { Apollo: '3', Hubane: '6', Juku: '4', Limpa: '5', Crocs: '9' },

  BANK_UNKNOWN_ALERT_THRESHOLD: 10,   // если новых неизвестных >= 10 -> тревога
  BANK_FAIL_ALERT_THRESHOLD: 1,       // если ошибок парсинга/CSV >= 1 -> тревога


  // ===== HEALTH / MONITORING =====
  HEALTH_MAX_SILENCE_MIN: 120,     // если > 120 минут не было успешного runFullCheck -> тревога
  HEALTH_CHECK_EVERY_HOURS: 1,     // как часто проверять "жив ли скрипт"
  HEALTH_SHEET_NAME: 'HEALTH',     // лист для кратких отчётов
  HEALTH_LOG_TO_SHEET: true,       // писать в лист HEALTH


  PREPAY_LINE_KEYWORDS: ['Tasutud ettemaks'],

  BANK_BLACKLIST: ['CHILLI', 'BOLTFOOD', 'BARBORA', 'INTRESS', 'SISSEMAKSE', 'TERM.ARV', 'LOHESABA'],

  COMPANY_NAME: 'Lohesaba (MJD Expert OÜ)',
  COMPANY_EMAIL: 'info@lohesaba.eu',
  CONTACT_PHONE: '+372 51 934 834',
  COMPANY_IBAN: 'EE542200221080357565',
  BANK_NAME: 'Swedbank',
  RULES_URL: 'https://lohesaba.eu/reeglid/',

  SHEET_COLS: 19,
  UNKNOWN_SEEN_MAX: 5000,

  AUTO_BACKUP_ON_RUN: false,
  LOG_TO_SHEET: true,

  // письма включатся автоматически после 20.02
  EMAILS_DISABLE_UNTIL: new Date(2026, 1, 20), // 20 Feb 2026
};

/** ========= MENU ========= */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('⚙️ Lohesaba Система')
    .addItem('📥 Запустить бэкап сейчас', 'dailyFullBackup')
    .addItem('💰 Банк + проверки (долг/конфликты/цвета)', 'runFullCheck')
    .addSeparator()
    .addItem('📤 ВОССТАНОВИТЬ из этой таблицы', 'restoreFromSheet')
    .addItem('🧹 Очистить старые бэкапы', 'cleanupOldBackups')
    .addSeparator()
    .addItem('⏱️ Установить автоматизацию (триггеры)', 'setupAutomationTriggers')
    .addToUi();
}

/** ========= MAIN ========= */
function runFullCheck() {
  healthMarkRunStart_();

  const summary = {
    bank_files: 0,
    bank_applied: 0,
    bank_dup: 0,
    bank_unmatched: 0,
    bookings: 0,
    conflicts: 0,
    debtors: 0,
    emails_confirm: 0,
    emails_admin: 0,
  };

  try {
    withLock_('runFullCheck', () => {
      log_('=== runFullCheck START ===');

      if (CONFIG.AUTO_BACKUP_ON_RUN) {
        log_('Auto-backup: creating backup...');
        dailyFullBackup_();
      } else {
        log_('Auto-backup: OFF (ok).');
      }

      const bankRes = processBankCSV_();              // <-- теперь вернёт статистику
      if (bankRes) {
        summary.bank_files = bankRes.files || 0;
        summary.bank_applied = bankRes.applied || 0;
        summary.bank_dup = bankRes.dup || 0;
        summary.bank_unmatched = bankRes.unmatched || 0;
        summary.emails_confirm = bankRes.emails_confirm || 0;
        summary.emails_admin = bankRes.emails_admin || 0;
      }

      const calRes = updateCalendarStatuses_();       // <-- теперь вернёт статистику
      if (calRes) {
        summary.bookings = calRes.bookings || 0;
        summary.conflicts = calRes.conflicts || 0;
        summary.debtors = calRes.debtors || 0;
      }

      log_('=== runFullCheck DONE ===');
    });

    healthMarkRunOk_(summary);
    return summary;

  } catch (e) {
    healthMarkRunFail_(e, summary);
    throw e;
  }
}


/** ========= BACKUP ========= */
function dailyFullBackup() {
  withLock_('dailyFullBackup', () => dailyFullBackup_());
}

function dailyFullBackup_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const stamp = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd_HH-mm');
  const folder = DriveApp.getFolderById(CONFIG.BACKUP_FOLDER_ID);

  const newFile = DriveApp.getFileById(ss.getId()).makeCopy('Backup_' + stamp, folder);
  const newSS = SpreadsheetApp.open(newFile);
  const newSheet = newSS.getSheets()[0];

  if (newSheet.getLastRow() > 1) {
    newSheet.getRange(2, 1, newSheet.getMaxRows() - 1, Math.max(CONFIG.SHEET_COLS, 50)).clearContent();
  }

  const calendar = CalendarApp.getCalendarById(CONFIG.CALENDAR_ID);
  const range = getMainRange_();
  const events = calendar.getEvents(range.start, range.end);

  const rows = [];
  events.forEach(event => {
    const title = event.getTitle();
    const desc = event.getDescription() || '';

    const startStr = Utilities.formatDate(event.getStartTime(), CONFIG.TIMEZONE, 'dd.MM.yyyy HH:mm');
    const endStr = Utilities.formatDate(event.getEndTime(), CONFIG.TIMEZONE, 'HH:mm');

    const prepayLine = extractPrepayLine_(desc);
    const manualPaid = extractManualPaidAmount_(desc);
    const autoPaid = getAutoPaidTotal_(desc);

    // Для отчёта/таблицы считаем оплачено и при наличке (ручная строка), и при банке.
    const hasPaid = (manualPaid + autoPaid) > 0;

    const email = getField_(desc, /(?:Kliendi e-mail|Email|Client e-mail):\s*([^\s\n\r]+)/i);
    const orderNum = getField_(desc, /(?:Arve№|Счет№|Invoice№|Arve nr)\s*([^\s\n\r]+)/i);

    rows.push([
      title,
      `${startStr} – ${endStr}`,
      getField_(desc, /(?:Lapse nimi|Имя ребенка|Child's name).*?:\s*([^\n\r]+)/i),
      getField_(desc, /(?:Kliendi tel\.|Телефон|Client tel\.).*?:\s*([^\n\r]+)/i),
      email,
      getField_(desc, /(?:Kliendi märkmed|Заметки|Client notes):\s*([\s\S]*?)(?=Kupongi|Soodus|Coupon|Toidu|Food|$)/i),
      getField_(desc, /(?:Kupongi|Soodus|Coupon):\s*([^\n\r]+)/i),
      getField_(desc, /(?:Peoruumihind|Стоимость комнаты|Room price):\s*([^\n\r]+)/i),
      getField_(desc, /(?:Toitlustuse hind|Стоимость питания|Catering):\s*([^\n\r]+)/i),
      getField_(desc, /(?:Summa kokku|Общая сумма|Total):\s*([^\n\r]+)/i),
      prepayLine,
      orderNum,
      '',
      '',
      '',
      desc,
      event.getColor() || '',
      hasPaid ? 'Paid' : 'Pending',
      Utilities.formatDate(event.getDateCreated() || new Date(), CONFIG.TIMEZONE, 'dd.MM.yyyy')
    ]);
  });

  if (rows.length) newSheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);

  cleanupOldBackups();
  log_(`Backup created: Backup_${stamp} (events=${events.length})`);
}

/** ========= BANK ========= */
function processBankCSV_() {
  log_('Bank: start...');
        let filesCount = 0;
        let totalDup = 0;
    let emailsConfirm = 0;
    let emailsAdmin = 0;
        let bad = 0; // битые CSV / неожиданные заголовки


    const calendar = CalendarApp.getCalendarById(CONFIG.CALENDAR_ID);
    const range = getMainRange_();

    const folder = DriveApp.getFolderById(CONFIG.BANK_FOLDER_ID);
    const files = folder.getFilesByType(MimeType.CSV);

    if (!files.hasNext()) {
  log_('Bank: no new CSV.');
  return;
}

    const unmatchedNew = [];
    let totalApplied = 0;

    while (files.hasNext()) {
      const file = files.next();
            filesCount++;
      log_('Bank: reading ' + file.getName());

      const blob = file.getBlob();
      let csvString = '';
      try { csvString = blob.getDataAsString('windows-1251'); }
      catch (e) { csvString = blob.getDataAsString('UTF-8'); }

      const data = Utilities.parseCsv(csvString, ';');
      if (!data || data.length < 2) {
        log_('Bank: invalid CSV: ' + file.getName());
                bad++;

        file.setTrashed(true);
        continue;
      }

            const header = data[0].map(x => String(x || '').trim());

      let col = null;
      try {
        col = getBankColumnMap_(header);
      } catch (e) {
        bad++;
        log_('Bank: CSV header not recognized in ' + file.getName() + ' | ' + e.message);
        file.setTrashed(true);
        continue;
      }


      let applied = 0, dup = 0;

      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        const payerName = upper_(getCell_(row, col.name));
        const selgitus = upper_(getCell_(row, col.desc));
        const archiveId = String(getCell_(row, col.archiveId) || '').trim();
        const debitKr = upper_(getCell_(row, col.debitKr));
        const amount = parseMoney_(String(getCell_(row, col.sum) || '0'));
        const payDate = String(getCell_(row, col.date) || '').trim();

        if (!(amount > 0)) continue;
        if (debitKr && debitKr !== 'C') continue;
        if (CONFIG.BANK_BLACKLIST.some(item => selgitus.includes(item) || payerName.includes(item))) continue;

        const orderNum = (selgitus.match(/\d{4}-\d{4}/) || [''])[0];

        if (!orderNum) {
          const key = makeUnknownKey_(archiveId, payDate, payerName, amount, selgitus);
          if (rememberUnknownIfNew_(key)) unmatchedNew.push({ name: payerName, amount, desc: selgitus, id: archiveId });
          continue;
        }

        const events = calendar.getEvents(range.start, range.end, { search: orderNum });
        if (!events.length) {
          const key = makeUnknownKey_(archiveId, payDate, payerName, amount, selgitus);
          if (rememberUnknownIfNew_(key)) unmatchedNew.push({ name: payerName, amount, desc: selgitus, id: archiveId });
          continue;
        }

        const event = events[0];

        const res = applyBankPaymentToEvent_(event, {
          amount,
          archiveId,
          payDate,
          selgitusRaw: selgitus,
        });

        if (res === 'duplicate') dup++;
        if (res === 'applied') {
          applied++;
          totalApplied++;
          clearDebtHint_(event);

          // подтверждение оплаты ТОЛЬКО после 20.02 и только 1 раз
          if (trySendConfirmationSafe_(event, orderNum, amount)) emailsConfirm++;
        }

        // в любом случае приводим порядок блоков (на случай старых событий)
        normalizePaymentLayout_(event);
      }

      file.setTrashed(true);
      log_(`Bank: done file ${file.getName()} | applied=${applied} dup=${dup}`);
    totalDup += dup;
    }

    // отчёт админу — после 20.02, только новые неизвестные
    if (unmatchedNew.length) {
  if (sendAdminReportSafe_(unmatchedNew)) emailsAdmin++;
}
    else log_('Admin report: no NEW unknown.');

    log_(`Bank: finished. totalApplied=${totalApplied}`);
        // авто-тревога если много unknown или были ошибки парсинга/CSV
    if (notifyAdminIfBankAnomaly_(
      { files: filesCount, applied: totalApplied, dup: totalDup, bad: bad },
      unmatchedNew
    )) {
      emailsAdmin++;
    }
    return {
      files: filesCount,
      applied: totalApplied,
      dup: totalDup,
      unmatched: unmatchedNew.length,
      bad: bad,
      emails_confirm: emailsConfirm,
      emails_admin: emailsAdmin,
    };
  }

/**
 * Применение банковского платежа:
 * - НЕ трогаем "Tasutud ettemaks:"
 * - читаемые строки сразу после "Tasutud ettemaks:"
 * - SYSTEM_PAYMENTS JSON в конце
 *
 * ВАЖНО: мы больше НЕ пытаемся сопоставлять с ручной строкой (matched_manual выключен).
 * Любой банковский платёж всегда считается applied.
 */
function applyBankPaymentToEvent_(event, p) {
  const now = new Date();
  if (event.getEndTime() < now) return 'duplicate';

  let desc = event.getDescription() || '';
  const sys = getSystemPayments_(desc);

  if (p.archiveId && sys.ids.has(p.archiveId)) return 'duplicate';

  const amount = Number(p.amount || 0);

  const item = {
    id: p.archiveId || '',
    amount: amount,
    date: p.payDate || Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd'),
    mode: 'applied',
    note: (p.selgitusRaw || '').slice(0, 120),
  };

  sys.list.push(item);
  if (item.id) sys.ids.add(String(item.id));
  sys.total += amount;

  // пересоберём описание “по стандарту”
  desc = rebuildPaymentLayout_(desc, sys.list);
  event.setDescription(desc);

  log_(`Bank: applied +€${amount.toFixed(2)} autoTotal=€${sys.total.toFixed(2)} | ${event.getTitle()}`);
  return 'applied';
}

/** ========= NORMALIZE PAYMENT LAYOUT ========= */
function normalizePaymentLayout_(event) {
  const desc = event.getDescription() || '';
  const sys = getSystemPayments_(desc);
  if (!sys.list.length) return; // нечего нормализовать
  const rebuilt = rebuildPaymentLayout_(desc, sys.list);
  if (rebuilt !== desc) event.setDescription(rebuilt);
}

function rebuildPaymentLayout_(desc, paymentList) {
  // 1) убираем старые auto-readable строки
  let s = removeAutoReadableLines_(desc);

  // 2) убираем SYSTEM_PAYMENTS блок
  s = removeBlock_(s, '--- SYSTEM_PAYMENTS ---', '--- END SYSTEM_PAYMENTS ---');

  // 3) вставляем auto-readable сразу после Tasutud ettemaks
  s = insertAutoReadableAfterManual_(s, paymentList);

  // 4) SYSTEM_PAYMENTS JSON — в самый конец
  s = setSystemPaymentsToEnd_(s, paymentList);

  return s;
}

function removeAutoReadableLines_(desc) {
  const lines = String(desc || '').split('\n');
  const cleaned = lines.filter(l =>
    !l.startsWith('Automaatne makse kokku:') &&
    !l.startsWith('Automaatne makse:')
  );
  return cleaned.join('\n').trim();
}

function insertAutoReadableAfterManual_(desc, list) {
  const lines = String(desc || '').split('\n');
  const manualIdx = findIndexOfLine_(lines, (l) => l.toUpperCase().includes('TASUTUD ETTEMAKS'));

  // total only applied
  let total = 0;
  list.forEach(p => { if (p && p.mode === 'applied' && typeof p.amount === 'number') total += p.amount; });

  const block = [];
  block.push(`Automaatne makse kokku: €${formatEu_(total)}`);

  // последние 5 платежей без mode
  const tail = list.slice(-5);
  tail.forEach(p => {
    const amt = (p && typeof p.amount === 'number') ? `€${formatEu_(p.amount)}` : '€0,00';
    const id = p && p.id ? `ID:${p.id}` : 'ID:-';
    const dt = p && p.date ? p.date : '-';
    block.push(`Automaatne makse: ${amt} | ${id} | ${dt}`);
  });

  if (manualIdx !== -1) {
    lines.splice(manualIdx + 1, 0, ...block);
    return lines.join('\n').trim();
  }

  // fallback: после "Summa kokku"
  const afterTotal = findIndexOfLine_(lines, (l) => /summa kokku/i.test(l));
  const insertAt = (afterTotal !== -1) ? afterTotal + 1 : lines.length;
  lines.splice(insertAt, 0, ...block);
  return lines.join('\n').trim();
}

function setSystemPaymentsToEnd_(desc, paymentList) {
  const markerStart = '--- SYSTEM_PAYMENTS ---';
  const markerEnd = '--- END SYSTEM_PAYMENTS ---';

  let s = removeBlock_(String(desc || ''), markerStart, markerEnd);

  const payload = { payments: paymentList.slice(-200) };
  const block = `${markerStart}
${JSON.stringify(payload)}
${markerEnd}`;

  s = (s.trim() + '\n\n' + block).trim();
  return s;
}

function findIndexOfLine_(lines, predicate) {
  for (let i = 0; i < lines.length; i++) {
    if (predicate(String(lines[i] || ''))) return i;
  }
  return -1;
}

/** ========= SYSTEM_PAYMENTS PARSE ========= */
function getSystemPayments_(desc) {
  const markerStart = '--- SYSTEM_PAYMENTS ---';
  const markerEnd = '--- END SYSTEM_PAYMENTS ---';
  const s = String(desc || '');
  const start = s.indexOf(markerStart);
  const end = start === -1 ? -1 : s.indexOf(markerEnd, start);

  const list = [];
  const ids = new Set();
  let total = 0;

  if (start !== -1 && end !== -1) {
    const inside = s.slice(start + markerStart.length, end).trim();
    try {
      const obj = JSON.parse(inside || '{}');
      const payments = Array.isArray(obj.payments) ? obj.payments : [];
      payments.forEach(p => {
        if (!p) return;
        list.push(p);
        if (p.id) ids.add(String(p.id));
        if (p.mode === 'applied' && typeof p.amount === 'number') total += p.amount;
      });
    } catch (e) {}
  }

  return { list, ids, total };
}

function getAutoPaidTotal_(desc) {
  return getSystemPayments_(desc).total || 0;
}

/** ========= STATUS CHECKS ========= */
function updateCalendarStatuses_() {
  log_('Calendar: updating...');
  const calendar = CalendarApp.getCalendarById(CONFIG.CALENDAR_ID);
  const range = getMainRange_();
  const events = calendar.getEvents(range.start, range.end);
  const bookings = events.filter(isBookingEvent_);

  // normalize payment layout for all bookings with system_payments
  bookings.forEach(e => normalizePaymentLayout_(e));

  // conflicts
  const conflicts = computeConflicts_(bookings);
  const conflictIds = new Set();
  conflicts.forEach(c => { conflictIds.add(c.a.getId()); conflictIds.add(c.b.getId()); });
  conflicts.forEach(c => { markConflict_(c.a, c.b); markConflict_(c.b, c.a); });
  bookings.forEach(e => { if (!conflictIds.has(e.getId())) clearConflictMark_(e); });

  const now = new Date();
  let debtCount = 0;

  bookings.forEach(event => {
    const title = event.getTitle();
    const desc = event.getDescription() || '';
    const created = event.getDateCreated() || now;
    const hoursPast = (now - created) / (1000 * 60 * 60);

    if (conflictIds.has(event.getId())) {
      safeSetColor_(event, CONFIG.COLOR_CONFLICT);
      return;
    }

    const autoPaid = getAutoPaidTotal_(desc);
    const manualOk = hasManualPaidMark_(desc);

    // должник только если нет авто И нет ручной отметки
    const isDebtor = (!manualOk) && (autoPaid <= 0.001) && (hoursPast >= CONFIG.DEBT_HOURS);

    if (isDebtor) {
      debtCount++;
      safeSetColor_(event, CONFIG.COLOR_DEBTOR);
      ensureDebtHint_(event);
    } else {
      clearDebtHint_(event);
      safeSetColor_(event, getRoomColor_(title));
    }
  });

  log_(`Calendar: done. bookings=${bookings.length} conflicts=${conflicts.length} debtors=${debtCount}`);

  return { bookings: bookings.length, conflicts: conflicts.length, debtors: debtCount };
}

/** ========= DEBT BLOCK ========= */
function ensureDebtHint_(event) {
  let desc = event.getDescription() || '';
  const markerStart = '--- SYSTEM DEBT ---';
  const markerEnd = '--- END SYSTEM DEBT ---';

  const orderNum = extractOrderNum_(desc);
  const room = detectRoom_(event.getTitle());
  const when =
    Utilities.formatDate(event.getStartTime(), CONFIG.TIMEZONE, 'dd.MM.yyyy HH:mm') +
    ' – ' + Utilities.formatDate(event.getEndTime(), CONFIG.TIMEZONE, 'HH:mm');

  const email = extractClientEmail_(desc);
  const totalLine = extractTotalLine_(desc) || '';

  // hint for prepayment: if manual line shows 0 -> default 35
  const manualLine = extractPrepayLine_(desc);
  let prepayHint = '€35,00';
  const m = String(manualLine || '').match(/€\s*([-]?\s*\d+(?:[.,]\d+)?)/);
  if (m) {
    const val = Math.abs(parseMoney_(m[1]));
    prepayHint = `€${formatEu_(val > 0 ? val : 35)}`;
  }

  const deadlineDate = new Date(Date.now() + 24 * 60 * 60 * 1000);
  const deadlineStr = Utilities.formatDate(deadlineDate, CONFIG.TIMEZONE, 'dd.MM.yyyy HH:mm');

  const gmailLink = buildDebtGmailComposeLink_(email, {
    orderNum, room, when, totalLine, deadlineStr, prepayHint
  });

  const block = [
    markerStart,
    `⚠️ Ettemaks puudub (üle ${CONFIG.DEBT_HOURS}h).`,
    `Palun saada kliendile meeldetuletus:`,
    gmailLink,
    markerEnd
  ].join('\n');

  desc = removeBlock_(desc, markerStart, markerEnd);
  desc = (desc.trim() + '\n\n' + block).trim();
  event.setDescription(desc);
}

function clearDebtHint_(event) {
  const markerStart = '--- SYSTEM DEBT ---';
  const markerEnd = '--- END SYSTEM DEBT ---';
  const desc = event.getDescription() || '';
  const cleaned = removeBlock_(desc, markerStart, markerEnd);
  if (cleaned !== desc) event.setDescription(cleaned);
}

function buildDebtGmailComposeLink_(email, ctx) {
  if (!email) return '(e-mail puudub)';

  const subject = `Meeldetuletus: ettemaks puudub (Arve ${ctx.orderNum || ''})`.trim();

  const body =
`Tere!

Meie süsteem ei leidnud Teie broneeringu ettemaksu laekumist.

Palun tasuge ettemaks ${ctx.prepayHint} ning võtke meiega ühendust, kui makse on juba tehtud.

Broneeringu andmed:
• Ruum: ${ctx.room}
• Aeg: ${ctx.when}
• Arve nr: ${ctx.orderNum || '-'}
${ctx.totalLine ? `• Summa kokku: ${ctx.totalLine}` : ''}

Täiendav tasumise tähtaeg: ${ctx.deadlineStr} (24h).
Kui ettemaks ei laeku ja Te ei võta meiega ühendust, jätame endale õiguse broneering tühistada.

Makse andmed:
• Saaja: ${CONFIG.COMPANY_NAME}
• Konto (IBAN): ${CONFIG.COMPANY_IBAN} (${CONFIG.BANK_NAME})
• Selgitus: Arve ${ctx.orderNum || ''}

Reeglid: ${CONFIG.RULES_URL}

Kontakt: ${CONFIG.CONTACT_PHONE} · ${CONFIG.COMPANY_EMAIL}

Lohesaba`;

  const base = 'https://mail.google.com/mail/?view=cm&fs=1';
  return base +
    '&to=' + encodeURIComponent(email) +
    '&su=' + encodeURIComponent(subject) +
    '&body=' + encodeURIComponent(body);
}

/** ========= EMAILS (REAL AFTER 20.02) ========= */
function emailsEnabledNow_() {
  return new Date() >= CONFIG.EMAILS_DISABLE_UNTIL;
}

function trySendConfirmationSafe_(event, orderNum, amount) {
  if (!emailsEnabledNow_()) {
    log_(`Email: disabled -> confirmation skipped (${orderNum})`);
    return false;
  }

  try {
    const now = new Date();
    if (event.getEndTime() < now) return false;

    const desc = event.getDescription() || '';
    const email = extractClientEmail_(desc);
    if (!email) return false;

    if (hasMailMarker_(desc, 'CONFIRM_SENT')) return false;

    const title = event.getTitle();
    const room = detectRoom_(title);
    const when =
      Utilities.formatDate(event.getStartTime(), CONFIG.TIMEZONE, 'dd.MM.yyyy HH:mm') +
      ' – ' + Utilities.formatDate(event.getEndTime(), CONFIG.TIMEZONE, 'HH:mm');

    sendConfirmationEmail_(email, orderNum, room, when, amount);
    addMailMarker_(event, 'CONFIRM_SENT');
    log_(`Email: confirmation sent -> ${email} (${orderNum})`);
    return true;

  } catch (e) {
    log_('Email ERROR (confirmation): ' + e.message);
    return false;
  }
}

function sendAdminReportSafe_(payments) {
  if (!emailsEnabledNow_()) {
    log_(`Email: disabled -> admin report skipped (new=${payments.length})`);
    return false;
  }

  try {
    sendAdminReport_(payments);
    log_(`Email: admin report sent (new=${payments.length})`);
    return true;
  } catch (e) {
    log_('Email ERROR (admin report): ' + e.message);
    return false;
  }
}

/** NEW confirmation email template */
function sendConfirmationEmail_(to, orderNum, room, when, amount) {
  const subject = `Broneering kinnitatud (Broneering nr ${orderNum || ''})`.trim();

  const paid = `€${Number(amount || 0).toFixed(2)}`;
  const roomLabel = `${room} peoruum`;

  const htmlBody = `
    <p>Tere!</p>

    <p>
      Täname õigeaegse tasumise eest! Teie makse summas <b>${paid}</b> on edukalt laekunud.
    </p>

    <p>
      Teie broneering nr <b>${escapeHtml_(orderNum || '')}</b> on kinnitatud.<br/>
      Valitud peoruum: <b>${escapeHtml_(roomLabel)}</b><br/>
      Aeg: <b>${escapeHtml_(when)}</b>
    </p>

    <p><b>Kasulikud lingid peo planeerimiseks:</b></p>

    <p style="margin:0 0 10px 0;">
      📩 <a href="https://lohesaba.eu/sunnipaevakutsed/" target="_blank" rel="noopener">Sünnipäevakutsed</a><br/>
      <span style="color:#777; font-size:12px;">(kohapeal saab osta ka trükitud kutsed)</span>
    </p>

    <p style="margin:0 0 10px 0;">
      🎭 <a href="https://lohesaba.eu/lisateenused/" target="_blank" rel="noopener">Peojuhid ja Näomaalijad</a><br/>
      <span style="color:#777; font-size:12px;">(tellimiseks tuleb ise kontakti võtta peojuhiga või näomaalijaga)</span>
    </p>

    <p style="margin:0 0 10px 0;">
      🍕 <a href="https://lohesaba.eu/peolaua-pakkettid/" target="_blank" rel="noopener">Peolaua paketid</a><br/>
      <span style="color:#777; font-size:12px;">(saab tellida hiljemalt 2 päeva enne üritust kirjutades meile kirja)</span>
    </p>

    <p style="margin:0 0 10px 0;">
      📜 <a href="https://lohesaba.eu/reeglid/" target="_blank" rel="noopener">Lohesaba reeglid</a><br/>
      <span style="color:#777; font-size:12px;">(broneeringut tehes kinnitate, et olete meie tingimustega tutvunud ja nõustute nendega)</span>
    </p>

    <p>
      Palume arvestada, et Lohesaba seikluslinnakus on lubatud viibida ainult sokkides või võimlemissussides.
      Soovitame tungivalt kasutada libisemisvastaseid sokke.
    </p>

    <p>Kohtumiseni Lohesaba seikluslinnakus!</p>
  `;

  GmailApp.sendEmail(to, subject, '', {
  htmlBody,
  from: CONFIG.COMPANY_EMAIL,
  name: CONFIG.COMPANY_NAME
});
}

function sendAdminReport_(payments) {
  let tableRows = '';
  payments.forEach(p => {
    tableRows += `
      <tr>
        <td style="border:1px solid #ddd; padding:8px;">${escapeHtml_(p.name)}</td>
        <td style="border:1px solid #ddd; padding:8px;"><b>€${Number(p.amount).toFixed(2)}</b></td>
        <td style="border:1px solid #ddd; padding:8px;">${escapeHtml_(p.desc)}</td>
        <td style="border:1px solid #ddd; padding:8px; font-size:10px;">${escapeHtml_(p.id || '')}</td>
      </tr>`;
  });

  const htmlBody = `
    <h2>⚠️ Uued tundmatud maksed</h2>
    <p>Süsteem ei suutnud järgmisi uusi makseid ühegi broneeringuga siduda:</p>
    <table style="border-collapse:collapse; width:100%;">
      <thead>
        <tr style="background:#f2f2f2;">
          <th style="border:1px solid #ddd; padding:8px;">Klient</th>
          <th style="border:1px solid #ddd; padding:8px;">Summa</th>
          <th style="border:1px solid #ddd; padding:8px;">Selgitus</th>
          <th style="border:1px solid #ddd; padding:8px;">Arhiivitunnus</th>
        </tr>
      </thead>
      <tbody>${tableRows}</tbody>
    </table>
  `;

  GmailApp.sendEmail(CONFIG.ADMIN_EMAIL, 'Hoiatus: Uued tundmatud maksed (Lohesaba)', '', {
  htmlBody,
  from: CONFIG.COMPANY_EMAIL,
  name: CONFIG.COMPANY_NAME
});
}

function notifyAdminIfBankAnomaly_(stats, unmatchedNew) {
  const unknownCount = (unmatchedNew && unmatchedNew.length) ? unmatchedNew.length : 0;
  const badCount = Number(stats && stats.bad ? stats.bad : 0);

  const unknownLimit = Number(CONFIG.BANK_UNKNOWN_ALERT_THRESHOLD || 10);
  const badLimit = Number(CONFIG.BANK_FAIL_ALERT_THRESHOLD || 1);

  if (unknownCount < unknownLimit && badCount < badLimit) return false;

  const subject = `⚠️ Lohesaba: аномалия банка (unknown=${unknownCount}, errors=${badCount})`;

  const top = (unmatchedNew || []).slice(0, 12).map(p =>
    `• €${Number(p.amount || 0).toFixed(2)} | ${p.name || '-'} | ${String(p.desc || '').slice(0, 80)} | ID:${p.id || '-'}`
  ).join('\n');

  const body =
`Обнаружена аномалия при разборе выписок.

Файлов обработано: ${stats.files || 0}
Применено платежей: ${stats.applied || 0}
Дубликатов: ${stats.dup || 0}
Новых неизвестных: ${unknownCount}
Ошибок/битых CSV: ${badCount}

Топ неизвестных (до 12):
${top || '(нет)'}
`;

  try {
  GmailApp.sendEmail(CONFIG.ADMIN_EMAIL, subject, body, {
  from: CONFIG.COMPANY_EMAIL,
  name: CONFIG.COMPANY_NAME
});
    return true;
  } catch (e) {
    log_('Bank anomaly mail ERROR: ' + e.message);
    return false;
  }
}


/** ========= MAIL MARKERS ========= */
function hasMailMarker_(desc, markerKey) {
  return String(desc || '').includes(markerKey + ':');
}

function addMailMarker_(event, markerKey) {
  const markerStart = '--- SYSTEM MAIL ---';
  const markerEnd = '--- END SYSTEM MAIL ---';
  let desc = event.getDescription() || '';

  const stamp = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd HH:mm:ss');
  const block = `${markerStart}\n${markerKey}:${stamp}\n${markerEnd}`;

  desc = removeBlock_(desc, markerStart, markerEnd);
  desc = (desc.trim() + '\n\n' + block).trim();
  event.setDescription(desc);
}

/** ========= CONFLICTS ========= */
function computeConflicts_(bookings) {
  const byRoom = {};
  bookings.forEach(e => {
  const room = detectRoom_(e.getTitle());
  if (room === 'Other') return;
  if (!byRoom[room]) byRoom[room] = [];
  byRoom[room].push(e);
});

  const bufferMs = CONFIG.CONFLICT_BUFFER_MIN * 60 * 1000;
  const result = [];

  Object.keys(byRoom).forEach(room => {
    const list = byRoom[room].slice().sort((a, b) => a.getStartTime() - b.getStartTime());
    for (let i = 0; i < list.length; i++) {
      for (let j = i + 1; j < list.length; j++) {
        const a = list[i], b = list[j];
        if (b.getStartTime().getTime() >= a.getEndTime().getTime() + bufferMs) break;

        const aStart = a.getStartTime().getTime();
        const aEnd = a.getEndTime().getTime() + bufferMs;
        const bStart = b.getStartTime().getTime();
        const bEnd = b.getEndTime().getTime() + bufferMs;

        if (aStart < bEnd && bStart < aEnd) result.push({ a, b, room });
      }
    }
  });

  return result;
}

function markConflict_(event, otherEvent) {
  let desc = event.getDescription() || '';
  const markerStart = '--- SYSTEM CONFLICT ---';
  const markerEnd = '--- END SYSTEM CONFLICT ---';

  const otherInfo =
    `${Utilities.formatDate(otherEvent.getStartTime(), CONFIG.TIMEZONE, 'dd.MM.yyyy HH:mm')} – ` +
    `${Utilities.formatDate(otherEvent.getEndTime(), CONFIG.TIMEZONE, 'HH:mm')} | ${otherEvent.getTitle()}`;

  const block = `${markerStart}
⚠️ Konflikt: vähem kui ${CONFIG.CONFLICT_BUFFER_MIN} min vahe
Teine broneering: ${otherInfo}
${markerEnd}`;

  desc = removeBlock_(desc, markerStart, markerEnd);
  desc = (desc.trim() + '\n\n' + block).trim();
  event.setDescription(desc);
}

function clearConflictMark_(event) {
  const markerStart = '--- SYSTEM CONFLICT ---';
  const markerEnd = '--- END SYSTEM CONFLICT ---';
  const desc = event.getDescription() || '';
  const cleaned = removeBlock_(desc, markerStart, markerEnd);
  if (cleaned !== desc) event.setDescription(cleaned);
}

/** ========= RESTORE ========= */
function restoreFromSheet() {
  withLock_('restoreFromSheet', () => {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    const calendar = CalendarApp.getCalendarById(CONFIG.CALENDAR_ID);
    const data = sheet.getDataRange().getValues();
    const restoreDate = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'dd.MM.yyyy');

    const range = getMainRange_();
    let created = 0;
    let skipped = 0;
    let bad = 0;

    for (let i = 1; i < data.length; i++) {
      const title = String(data[i][0] || '').trim();
      const timeCell = data[i][1];
      const fullDesc = String(data[i][15] || '');
      const color = String(data[i][16] || '').trim();

      if (!title) { skipped++; continue; }

      const parsed = parseSheetTimeCell_(timeCell);
      if (!parsed) {
        bad++;
        log_(`Restore: bad time format at row ${i + 1}. Col B value = "${String(timeCell || '').trim()}"`);
        continue;
      }
      const { startTime, endTime } = parsed;

      try {
        const orderNum = getField_(fullDesc, /(?:Arve№|Arve nr|Счет№|Invoice№)\s*(\d{4}-\d{4})/i);
        if (orderNum) {
          const existingByInvoice = calendar.getEvents(range.start, range.end, { search: orderNum });
          if (existingByInvoice && existingByInvoice.length) {
            skipped++;
            continue;
          }
        }

        const existingSameTime = calendar.getEvents(startTime, endTime, { search: title });
        if (existingSameTime && existingSameTime.length) {
          skipped++;
          continue;
        }

        const event = calendar.createEvent(title, startTime, endTime, {
          description: `⚠️ [RESTORED ${restoreDate}]\n\n` + fullDesc
        });

        if (color) event.setColor(color);
        created++;

      } catch (e) {
        bad++;
        log_(`Restore error row ${i + 1}: ${e.message}`);
      }
    }

    SpreadsheetApp.getUi().alert(
      `Taastamine lõpetatud!\nLoodud: ${created}\nVahele jäetud: ${skipped}\nVigased read: ${bad}`
    );
    log_(`Restore done. created=${created} skipped=${skipped} bad=${bad}`);
  });
}

function parseSheetTimeCell_(cell) {
  if (!cell) return null;

  if (Object.prototype.toString.call(cell) === '[object Date]' && !isNaN(cell.getTime())) {
    const startTime = cell;
    const endTime = new Date(startTime.getTime() + 3 * 60 * 60 * 1000);
    return { startTime, endTime };
  }

  const s = String(cell).trim();
  if (!s) return null;

  const normalized = s.replace(/\s+/g, ' ').replace(/—/g, '-').replace(/–/g, '-');

  const m = normalized.match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})$/);
  if (!m) return null;

  const dd = Number(m[1]);
  const mm = Number(m[2]);
  const yyyy = Number(m[3]);
  const sh = Number(m[4]);
  const sm = Number(m[5]);
  const eh = Number(m[6]);
  const em = Number(m[7]);

  const startTime = new Date(yyyy, mm - 1, dd, sh, sm);
  const endTime = new Date(yyyy, mm - 1, dd, eh, em);

  if (endTime.getTime() <= startTime.getTime()) {
    endTime.setDate(endTime.getDate() + 1);
  }

  return { startTime, endTime };
}

/** ========= CLEANUP ========= */
function cleanupOldBackups() {
  const folder = DriveApp.getFolderById(CONFIG.BACKUP_FOLDER_ID);
  const files = folder.getFiles();
  const now = new Date();
  let removed = 0;

  while (files.hasNext()) {
    const file = files.next();
    if (now - file.getDateCreated() > CONFIG.DAYS_TO_KEEP_BACKUPS * 24 * 60 * 60 * 1000) {
      file.setTrashed(true);
      removed++;
    }
  }
  log_(`Backup cleanup done. Removed=${removed}`);
}

/** ========= BOOKING DETECTION & FIELDS ========= */
function isBookingEvent_(event) {
  const desc = event.getDescription() || '';
  return /(?:Arve№|Arve nr|Счет№|Invoice№)\s*\d{4}-\d{4}/i.test(desc);
}

function extractOrderNum_(desc) {
  const m = String(desc || '').match(/(?:Arve№|Arve nr|Счет№|Invoice№)\s*(\d{4}-\d{4})/i);
  return m ? m[1] : '';
}

function extractClientEmail_(desc) {
  const m = String(desc || '').match(/(?:Kliendi e-mail|Email|Client e-mail):\s*([^\s\n\r]+)/i);
  return m ? m[1].trim() : '';
}

function extractTotalLine_(desc) {
  const m = String(desc || '').match(/(?:Summa kokku|Общая сумма|Total):\s*([^\n\r]+)/i);
  return m ? m[1].trim() : '';
}

function extractPrepayLine_(desc) {
  return findLine_(desc, CONFIG.PREPAY_LINE_KEYWORDS) || '';
}

function extractManualPaidAmount_(desc) {
  const line = findLine_(desc, CONFIG.PREPAY_LINE_KEYWORDS);
  if (!line) return 0;
  let m = line.match(/€\s*([-]?\s*\d+(?:[.,]\d+)?)/);
  if (!m) m = line.match(/([-]?\s*\d+(?:[.,]\d+)?)/);
  if (!m) return 0;
  return Math.abs(parseMoney_(m[1]));
}

/** ========= UNKNOWN MEMORY ========= */
function makeUnknownKey_(archiveId, payDate, payerName, amount, selgitus) {
  if (archiveId) return 'ID:' + archiveId;
  const base = [payDate || '-', payerName || '-', String(amount || 0), selgitus || '-'].join('|');
  return 'H:' + Utilities.base64EncodeWebSafe(Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, base)).slice(0, 32);
}

function rememberUnknownIfNew_(key) {
  const props = PropertiesService.getScriptProperties();
  const raw = props.getProperty('UNKNOWN_SEEN_KEYS');
  let arr = [];
  try { arr = raw ? JSON.parse(raw) : []; } catch (e) { arr = []; }

  const set = new Set(arr);
  if (set.has(key)) return false;

  arr.push(key);
  if (arr.length > CONFIG.UNKNOWN_SEEN_MAX) arr = arr.slice(arr.length - CONFIG.UNKNOWN_SEEN_MAX);
  props.setProperty('UNKNOWN_SEEN_KEYS', JSON.stringify(arr));
  return true;
}

/** ========= CSV COLUMNS ========= */
function getBankColumnMap_(header) {
  const norm = header.map(h => String(h || '').trim().toLowerCase());
  function find(names) {
    for (const n of names) {
      const idx = norm.indexOf(n.toLowerCase());
      if (idx !== -1) return idx;
    }
    return -1;
  }
  const map = {
    name: find(['saaja/maksja nimi', 'maksja nimi', 'nimi']),
    desc: find(['selgitus', 'selgitus / viide', 'kirjeldus']),
    sum: find(['summa', 'amount']),
    archiveId: find(['arhiveerimistunnus', 'arhiivitunnus']),
    debitKr: find(['deebet/kr', 'db/cr']),
    date: find(['makse kuupäev', 'kuupäev', 'date']),
  };
  if (map.sum === -1 || map.desc === -1) {
    throw new Error('CSV header not recognized. Need columns: Summa, Selgitus ...');
  }
  return map;
}

/** ========= UTILS ========= */
function getMainRange_() {
  const now = new Date();
  return {
    start: new Date(now.getTime() - CONFIG.LOOKBACK_DAYS * 24 * 60 * 60 * 1000),
    end: new Date(now.getTime() + CONFIG.LOOKAHEAD_DAYS * 24 * 60 * 60 * 1000),
  };
}

/**
 * Ручная отметка оплаты (наличка/вручную) через строку "Tasutud ettemaks".
 * ВАЖНО: безопасный режим — считаем оплатой ТОЛЬКО если указана сумма > 0.
 */
function hasManualPaidMark_(desc) {
  const line = extractPrepayLine_(desc);
  if (!line) return false;

  const m = String(line).match(/€\s*([-]?\s*\d+(?:[.,]\d+)?)/);
  if (!m) return false; // строго: без суммы — не считается оплатой
  return Math.abs(parseMoney_(m[1])) > 0.001;
}

function withLock_(name, fn) {
  const lock = LockService.getScriptLock();
  const ok = lock.tryLock(30000);
  if (!ok) throw new Error('Lock timeout: ' + name);
  try { fn(); } finally { lock.releaseLock(); }
}

function log_(message) {
  const ts = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd HH:mm:ss');
  const line = `[${ts}] ${message}`;
  console.log(line);

  try {
    const folder = DriveApp.getFolderById(CONFIG.LOG_FOLDER_ID);
    const name = 'Lohesaba_LOG_' + Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd') + '.txt';
    let file = null;
    const it = folder.getFilesByName(name);
    if (it.hasNext()) file = it.next();
    else file = folder.createFile(name, '');
    file.setContent(file.getBlob().getDataAsString('UTF-8') + line + '\n');
  } catch (e) {}

  if (CONFIG.LOG_TO_SHEET) {
    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      let sh = ss.getSheetByName('LOG');
      if (!sh) sh = ss.insertSheet('LOG');
      const msg = String(message || '');
const safeMsg = msg.startsWith('=') ? ("'" + msg) : msg; // принудительно текст
sh.appendRow([ts, safeMsg]);

// защита: убираем автозаполненные формулы/значения справа от B
const r = sh.getLastRow();
const maxCols = sh.getMaxColumns();
if (maxCols > 2) {
  sh.getRange(r, 3, 1, maxCols - 2).clearContent();
}
    } catch (e) {}
  }
}

function detectRoom_(title) {
  const t = String(title || '').toUpperCase();

  if (t.includes('APOLLO')) return 'Apollo';
  if (t.includes('HUBANE')) return 'Hubane';
  if (t.includes('JUKU')) return 'Juku';
  if (t.includes('LIMPA')) return 'Limpa';
  if (t.includes('CROCS')) return 'Crocs';

  return 'Other';
}

function getRoomColor_(title) {
  const room = detectRoom_(title);
  return CONFIG.ROOM_COLORS[room] || CONFIG.COLOR_DEFAULT;
}

function safeSetColor_(event, colorId) {
  const cur = event.getColor() || '';
  if (colorId && cur !== colorId) event.setColor(colorId);
}

function findLine_(desc, keywords) {
  const lines = String(desc || '').split('\n');
  for (const line of lines) {
    const up = line.toUpperCase();
    for (const k of keywords) {
      if (up.includes(String(k).toUpperCase())) return line;
    }
  }
  return '';
}

function getField_(text, reg) {
  const m = String(text || '').match(reg);
  return (m && m[1]) ? String(m[1]).trim() : '';
}

function parseMoney_(value) {
  const s = String(value || '').replace(/\s/g, '').replace(',', '.');
  const num = parseFloat(s);
  return isNaN(num) ? 0 : num;
}

function formatEu_(n) {
  return Number(n || 0).toFixed(2).replace('.', ',');
}

function upper_(s) { return String(s || '').toUpperCase(); }

function getCell_(row, idx) {
  if (idx === -1 || idx == null) return '';
  return row[idx];
}

function removeBlock_(text, startMarker, endMarker) {
  const s = String(text || '');
  const start = s.indexOf(startMarker);
  if (start === -1) return s;
  const end = s.indexOf(endMarker, start);
  if (end === -1) return s;
  const after = end + endMarker.length;
  return (s.slice(0, start).trim() + '\n' + s.slice(after).trim()).trim();
}

function escapeHtml_(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/** ========= HEALTH / MONITORING ========= */

const HEALTH_KEYS = {
  LAST_OK_MS: 'HEALTH_LAST_OK_MS',
  LAST_RUN_MS: 'HEALTH_LAST_RUN_MS',
  LAST_RUN_STATUS: 'HEALTH_LAST_RUN_STATUS',
  LAST_RUN_SUMMARY: 'HEALTH_LAST_RUN_SUMMARY',
  LAST_ALERT_MS: 'HEALTH_LAST_ALERT_MS',
  SPREADSHEET_ID: 'HEALTH_SPREADSHEET_ID',
};

function getSpreadsheet_() {
  // В триггерах getActiveSpreadsheet иногда бывает null — подстрахуемся свойством
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) return ss;
  } catch (e) {}

  const props = PropertiesService.getScriptProperties();
  const id = props.getProperty(HEALTH_KEYS.SPREADSHEET_ID);
  if (!id) return null;

  try { return SpreadsheetApp.openById(id); } catch (e) { return null; }
}

function ensureSpreadsheetIdStored_() {
  const props = PropertiesService.getScriptProperties();
  if (props.getProperty(HEALTH_KEYS.SPREADSHEET_ID)) return;

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) props.setProperty(HEALTH_KEYS.SPREADSHEET_ID, ss.getId());
  } catch (e) {}
}

function healthMarkRunStart_() {
  ensureSpreadsheetIdStored_();
  const props = PropertiesService.getScriptProperties();
  props.setProperty(HEALTH_KEYS.LAST_RUN_MS, String(Date.now()));
  props.setProperty(HEALTH_KEYS.LAST_RUN_STATUS, 'RUNNING');
}

function healthMarkRunOk_(summaryObj) {
  const props = PropertiesService.getScriptProperties();
  const now = Date.now();
  props.setProperty(HEALTH_KEYS.LAST_OK_MS, String(now));
  props.setProperty(HEALTH_KEYS.LAST_RUN_STATUS, 'OK');
  props.setProperty(HEALTH_KEYS.LAST_RUN_SUMMARY, JSON.stringify(summaryObj || {}));
  healthAppendRow_(summaryObj, 'OK');
}

function healthMarkRunFail_(err, summaryObj) {
  const props = PropertiesService.getScriptProperties();
  props.setProperty(HEALTH_KEYS.LAST_RUN_STATUS, 'FAIL');

  const payload = {
    ...(summaryObj || {}),
    error: String((err && err.message) ? err.message : err),
  };
  props.setProperty(HEALTH_KEYS.LAST_RUN_SUMMARY, JSON.stringify(payload));
  healthAppendRow_(payload, 'FAIL');
}

function healthAppendRow_(summaryObj, status) {
  if (!CONFIG.HEALTH_LOG_TO_SHEET) return;

  const ss = getSpreadsheet_();
  if (!ss) return;

  let sh = ss.getSheetByName(CONFIG.HEALTH_SHEET_NAME);
  if (!sh) sh = ss.insertSheet(CONFIG.HEALTH_SHEET_NAME);

  // заголовки (один раз)
  if (sh.getLastRow() === 0) {
    sh.appendRow([
      'Timestamp', 'Status',
      'bank_files', 'bank_applied', 'bank_dup', 'bank_unmatched',
      'bookings', 'conflicts', 'debtors',
      'emails_confirm', 'emails_admin',
      'note'
    ]);
  }

  const ts = Utilities.formatDate(new Date(), CONFIG.TIMEZONE, 'yyyy-MM-dd HH:mm:ss');

  const s = summaryObj || {};
  sh.appendRow([
    ts, status,
    s.bank_files || 0, s.bank_applied || 0, s.bank_dup || 0, s.bank_unmatched || 0,
    s.bookings || 0, s.conflicts || 0, s.debtors || 0,
    s.emails_confirm || 0, s.emails_admin || 0,
    (s.error || '')
  ]);
}

/**
 * Проверка, что скрипт "жив".
 * Срабатывает по триггеру раз в N часов и шлёт письмо,
 * если давно не было успешного runFullCheck.
 */
function healthCheck_() {
  const props = PropertiesService.getScriptProperties();
  const lastOk = Number(props.getProperty(HEALTH_KEYS.LAST_OK_MS) || 0);
  const lastAlert = Number(props.getProperty(HEALTH_KEYS.LAST_ALERT_MS) || 0);

  const now = Date.now();
  const maxSilenceMs = (CONFIG.HEALTH_MAX_SILENCE_MIN || 120) * 60 * 1000;

  // ещё не было успешных запусков — не тревожим
  if (!lastOk) return;

  const silence = now - lastOk;
  if (silence <= maxSilenceMs) return;

  // анти-спам: не чаще 1 раза в 6 часов
  if (lastAlert && (now - lastAlert) < 6 * 60 * 60 * 1000) return;

  const summaryRaw = props.getProperty(HEALTH_KEYS.LAST_RUN_SUMMARY) || '{}';
  let summary = {};
  try { summary = JSON.parse(summaryRaw); } catch (e) {}

  const silenceMin = Math.round(silence / 60000);

  const subject = `⚠️ Lohesaba: скрипт не выполнялся успешно ${silenceMin} мин`;
  const body =
`Внимание!

Не было успешного запуска runFullCheck уже: ${silenceMin} минут.

Последний статус: ${props.getProperty(HEALTH_KEYS.LAST_RUN_STATUS) || 'UNKNOWN'}
Последняя сводка:
${summaryRaw}

Рекомендации:
1) Открыть Apps Script → Executions / Журнал выполнения
2) Проверить триггеры (Triggers)
3) Проверить права доступа (Calendar/Drive/Mail)
`;

  try {
    GmailApp.sendEmail(CONFIG.ADMIN_EMAIL, subject, body, {
  from: CONFIG.COMPANY_EMAIL,
  name: CONFIG.COMPANY_NAME
});
  } catch (e) {
    // если почта не отправилась — хотя бы в лог
    log_('HealthCheck mail ERROR: ' + e.message);
  }

  props.setProperty(HEALTH_KEYS.LAST_ALERT_MS, String(now));
}


/** ========= AUTOMATION TRIGGERS =========
 * Запусти ОДИН раз вручную (Run), дальше будет работать автоматически.
 */
function setupAutomationTriggers() {
  // удалить ВСЕ старые триггеры проекта
  ScriptApp.getProjectTriggers().forEach(t => ScriptApp.deleteTrigger(t));

  // основной цикл: банк + статусы
  ScriptApp.newTrigger('runFullCheck')
    .timeBased()
    .everyMinutes(15)
    .create();

  // ночной backup
  ScriptApp.newTrigger('dailyFullBackup')
    .timeBased()
    .everyDays(1)
    .atHour(3)
    .nearMinute(15)
    .create();

  // чистка бэкапов
  ScriptApp.newTrigger('cleanupOldBackups')
    .timeBased()
    .everyDays(1)
    .atHour(3)
    .nearMinute(45)
    .create();

      // health-check (каждый час)
  ScriptApp.newTrigger('healthCheck_')
    .timeBased()
    .everyHours(CONFIG.HEALTH_CHECK_EVERY_HOURS || 1)
    .create();

    log_('Triggers installed: runFullCheck (15m), dailyFullBackup (daily), cleanupOldBackups (daily), healthCheck (hourly)');
}
