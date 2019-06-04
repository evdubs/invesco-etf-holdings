#lang racket/base

(require db
         racket/cmdline
         racket/sequence
         racket/string
         racket/system
         srfi/19) ; Time Data Types and Procedures

(struct etf-component
  (fund-ticker
   security-num
   holdings-ticker
   shares
   market-value
   weight
   name
   sector
   date)
  #:transparent)

(define base-folder (make-parameter "/var/tmp/invesco/etf-holdings"))

(define convert-xls (make-parameter #f))

(define folder-date (make-parameter (current-date)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket transform-load-csv.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "SPDR ETF Holdings base folder. Defaults to /var/tmp/invesco/etf-holdings"
                         (base-folder folder)]
 [("-c" "--convert-xls") "Convert XLS documents to CSV for handling. This requires libreoffice to be installed"
                         (convert-xls #t)]
 [("-d" "--folder-date") date
                         "Invesco ETF Holdings folder date. Defaults to today"
                         (folder-date (string->date date "~Y-~m-~d"))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(parameterize ([current-directory (string-append (base-folder) "/" (date->string (folder-date) "~1") "/")])
  (cond [(convert-xls)
         (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".xls")) (in-directory))])
           (system (string-append "libreoffice --headless --convert-to csv --outdir " (path->string (current-directory)) " " (path->string p))))])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".csv")) (in-directory))])
    (let ([file-name (string-append (base-folder) "/" (date->string (folder-date) "~1") "/" (path->string p))]
          [ticker-symbol (string-replace (path->string p) ".csv" "")])
      (call-with-input-file file-name
        (λ (in)
          (displayln file-name)
          (let* ([sheet-values (sequence->list (in-lines in))]
                 [rows (map (λ (r) (apply etf-component (regexp-split #rx"," r))) sheet-values)]
                 [filtered-rows (filter (λ (r) (not (equal? "FundTicker" (etf-component-fund-ticker r)))) rows)])
            (define insert-counter 0)
            (define insert-success-counter 0)
            (define insert-failure-counter 0)
            (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                        ticker-symbol
                                                                        " for date "
                                                                        (date->string (folder-date) "~1")))
                                         (displayln ((error-value->string-handler) e 1000))
                                         (rollback-transaction dbc)
                                         (set! insert-failure-counter (add1 insert-failure-counter)))])
              (for-each (λ (row)
                          (set! insert-counter (add1 insert-counter))
                          (start-transaction dbc)
                          (query-exec dbc "
insert into invesco.etf_holding
(
  etf_symbol,
  date,
  component_symbol,
  weight,
  sector,
  shares_held
) values (
  $1,
  $2::text::date,
  $3,
  $4::text::numeric,
  $5::text::invesco.sector,
  $6::text::numeric
) on conflict (etf_symbol, date, component_symbol) do nothing;
"
                                      ticker-symbol
                                      (date->string (folder-date) "~1")
                                      (string-replace (string-trim (etf-component-holdings-ticker row)) "/" ".")
                                      (etf-component-weight row)
                                      (etf-component-sector row)
                                      (etf-component-shares row))
                          (commit-transaction dbc)
                          (set! insert-success-counter (add1 insert-success-counter))) filtered-rows))
            (displayln (string-append "Attempted to insert " (number->string insert-counter) " rows. "
                                      (number->string insert-success-counter) " were successful. "
                                      (number->string insert-failure-counter) " failed."))))))))
