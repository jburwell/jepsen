(ns jepsen.system.riak2-test
  (:use jepsen.system.riak2
        jepsen.core
        jepsen.core-test
        clojure.test
        clojure.pprint)
  (:require [clojure.string   :as str]
            [jepsen.util      :as util]
            [jepsen.os.debian :as debian]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.model     :as model]
            [jepsen.generator :as gen]
            [jepsen.nemesis   :as nemesis]
            [jepsen.store     :as store]
            [jepsen.report    :as report]))

(deftest register-test
  (let [test (run!
              (assoc
                noop-test
                :name      "riak2-dt-set"
                :os        debian/os
                :db        db
                :client    (create-set-client)
                :model     (model/set)
                :checker   (checker/compose {:html   timeline/html
                                             :linear checker/linearizable})
                :nemesis   (nemesis/partition-random-halves)
                :generator (gen/phases
                            (->> gen/cas
                                 (gen/delay 1)
                                 (gen/nemesis
                                  (gen/seq
                                   (cycle [(gen/sleep 5)
                                           {:type :info :f :start}
                                           (gen/sleep 5)
                                           {:type :info :f :stop}])))
                                 (gen/time-limit 20))
                            (gen/nemesis
                             (gen/once {:type :info :f :stop}))
                            (gen/clients
                             (gen/once {:type :invoke :f :read})))))]
    (is (:valid? (:results test)))
        (report/linearizability (:linear (:results test)))))
