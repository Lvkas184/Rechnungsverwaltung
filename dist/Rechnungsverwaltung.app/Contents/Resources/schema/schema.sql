PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS parameters (
  param_name TEXT PRIMARY KEY,
  param_value TEXT
);

CREATE TABLE IF NOT EXISTS invoices (
  invoice_id INTEGER PRIMARY KEY,
  name TEXT,
  remark TEXT,
  document_type TEXT NOT NULL DEFAULT 'rechnung',
  credit_target_invoice_id INTEGER NULL,
  amount_gross REAL,
  issue_date TEXT,
  due_date TEXT,
  status TEXT,
  status_manual INTEGER DEFAULT 0,
  deviation_eur REAL,
  paid_sum_eur REAL DEFAULT 0,
  last_payment_date TEXT,
  payment_count INTEGER DEFAULT 0,
  action TEXT,
  reminder_status TEXT,
  reminder_date TEXT,
  reminder_manual INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(credit_target_invoice_id) REFERENCES invoices(invoice_id)
);

CREATE TABLE IF NOT EXISTS payments (
  payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
  invoice_id INTEGER NULL,
  parent_payment_id INTEGER NULL,
  source TEXT,
  booking_date TEXT,
  value_date TEXT,
  amount_eur REAL,
  reference_text TEXT,
  iban TEXT,
  beneficiary_name TEXT,
  remark TEXT,
  matched INTEGER DEFAULT 0,
  akonto INTEGER DEFAULT 0,
  schadensrechnung INTEGER DEFAULT 0,
  status_manual INTEGER DEFAULT 0,
  status_override TEXT,
  match_score REAL,
  match_rule TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(invoice_id) REFERENCES invoices(invoice_id)
);

CREATE TABLE IF NOT EXISTS manual_map (
  signature TEXT PRIMARY KEY,
  mapped_invoice_id INTEGER,
  updated_at TEXT,
  notes TEXT,
  FOREIGN KEY(mapped_invoice_id) REFERENCES invoices(invoice_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
  audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
  payment_id INTEGER,
  invoice_id INTEGER,
  match_score REAL,
  rule_used TEXT,
  automated INTEGER,
  user TEXT,
  ts TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(payment_id) REFERENCES payments(payment_id),
  FOREIGN KEY(invoice_id) REFERENCES invoices(invoice_id)
);

CREATE TABLE IF NOT EXISTS manual_change_log (
  change_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_audit_id INTEGER UNIQUE,
  entry_origin TEXT NOT NULL DEFAULT 'auto',
  is_resolved INTEGER DEFAULT 0,
  resolved_at TEXT,
  change_scope TEXT NOT NULL,
  invoice_id INTEGER,
  payment_id INTEGER,
  action_code TEXT NOT NULL,
  action_label TEXT NOT NULL,
  before_value TEXT,
  after_value TEXT,
  note TEXT,
  changed_by TEXT,
  changed_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(invoice_id) REFERENCES invoices(invoice_id),
  FOREIGN KEY(payment_id) REFERENCES payments(payment_id),
  FOREIGN KEY(source_audit_id) REFERENCES audit_log(audit_id)
);

CREATE INDEX IF NOT EXISTS idx_manual_change_log_changed_at
  ON manual_change_log(changed_at);

CREATE INDEX IF NOT EXISTS idx_manual_change_log_invoice
  ON manual_change_log(invoice_id);

CREATE INDEX IF NOT EXISTS idx_manual_change_log_payment
  ON manual_change_log(payment_id);

CREATE TABLE IF NOT EXISTS invoice_reminders (
  reminder_entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
  invoice_id INTEGER NOT NULL,
  reminder_status TEXT NOT NULL,
  reminder_date TEXT NOT NULL,
  manual_entry INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(invoice_id) REFERENCES invoices(invoice_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_invoice_reminders_invoice_stage
  ON invoice_reminders(invoice_id, reminder_status);

CREATE INDEX IF NOT EXISTS idx_invoice_reminders_invoice
  ON invoice_reminders(invoice_id);

CREATE TABLE IF NOT EXISTS import_batches (
  import_batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
  import_type TEXT NOT NULL,
  source_label TEXT NOT NULL,
  filename TEXT,
  created_by TEXT,
  row_count_imported INTEGER DEFAULT 0,
  row_count_skipped INTEGER DEFAULT 0,
  status TEXT DEFAULT 'active',
  rollback_note TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  rollback_at TEXT
);

CREATE TABLE IF NOT EXISTS import_batch_items (
  import_batch_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
  import_batch_id INTEGER NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id INTEGER NOT NULL,
  action TEXT NOT NULL,
  fields_touched TEXT,
  before_state TEXT,
  after_state TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(import_batch_id) REFERENCES import_batches(import_batch_id)
);

CREATE INDEX IF NOT EXISTS idx_import_batch_items_batch
  ON import_batch_items(import_batch_id);
