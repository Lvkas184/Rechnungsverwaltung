PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS parameters (
  param_name TEXT PRIMARY KEY,
  param_value TEXT
);

CREATE TABLE IF NOT EXISTS invoices (
  invoice_id INTEGER PRIMARY KEY,
  name TEXT,
  amount_gross REAL,
  issue_date TEXT,
  due_date TEXT,
  status TEXT,
  deviation_eur REAL,
  paid_sum_eur REAL DEFAULT 0,
  last_payment_date TEXT,
  payment_count INTEGER DEFAULT 0,
  action TEXT,
  reminder_status TEXT,
  reminder_date TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
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
  matched INTEGER DEFAULT 0,
  akonto INTEGER DEFAULT 0,
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
