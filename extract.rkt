#lang racket/base

(require gregor
         net/url
         racket/file
         racket/list
         racket/port
         tasks
         threading)

(define (download-etf-holdings symbol)
  (make-directory* (string-append "/var/tmp/invesco/etf-holdings/" (~t (today) "yyyy-MM-dd")))
  (call-with-output-file (string-append "/var/tmp/invesco/etf-holdings/" (~t (today) "yyyy-MM-dd") "/" symbol ".csv")
    (λ (out) (~> (string-append "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?ticker=" symbol "&action=download")
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
