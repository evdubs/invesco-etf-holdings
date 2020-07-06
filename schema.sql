CREATE SCHEMA invesco;

CREATE TYPE invesco.sector AS ENUM
   ('Communication Services',
    'Consumer Discretionary',
    'Consumer Staples',
    'Energy',
    'Financials',
    'Health Care',
    'Industrials',
    'Information Technology',
    'Materials',
    'Utilities');
    
CREATE TABLE invesco.etf_holding
(
  etf_symbol text NOT NULL,
  date date NOT NULL,
  component_symbol text NOT NULL,
  weight numeric NOT NULL,
  sector invesco.sector,
  shares_held numeric NOT NULL,
  CONSTRAINT etf_holding_pkey PRIMARY KEY (etf_symbol, date, component_symbol),
  CONSTRAINT etf_holding_component_symbol_fkey FOREIGN KEY (component_symbol)
      REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT etf_holding_etf_symbol_fkey FOREIGN KEY (etf_symbol)
      REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);
