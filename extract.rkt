#lang racket/base

(require net/url
         racket/file
         racket/list
         racket/port
         srfi/19 ; Time Data Types and Procedures
         tasks
         threading)

(define (download-etf-holdings symbol)
  (make-directory* (string-append "/var/tmp/invesco/etf-holdings/" (date->string (current-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/invesco/etf-holdings/" (date->string (current-date) "~1") "/" symbol ".xls")
    (λ (out) (~> (string-append "https://www.invesco.com/portal/site/us/template.BINARYPORTLET/financial-professional/etfs/holdings/resource.process/"
                     "?javax.portlet.tpst=72e337bf3b31ef1a015fe531524e2ca0"
                     "&javax.portlet.rid_72e337bf3b31ef1a015fe531524e2ca0=excelHoldings"
                     "&javax.portlet.rcl_72e337bf3b31ef1a015fe531524e2ca0=cacheLevelPage"
                     "&javax.portlet.begCacheTok=com.vignette.cachetoken"
                     "&javax.portlet.endCacheTok=com.vignette.cachetoken"
                     "&ts=" (date->string (current-date) "~4")
                     "&javax.portlet.prp_72e337bf3b31ef1a015fe531524e2ca0_ticker=" symbol)
                 (string->url _)
                 (get-pure-port _)
                 (copy-port _ out)))
    #:exists 'replace))

(define invesco-etfs (list "ADRA" "ADRD" "ADRE" "ADRU"))

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length invesco-etfs))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-etf-holdings (first l)))
                                                          (second l)))
                            (map list invesco-etfs delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
