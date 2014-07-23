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
            [jepsen.report    :as report])
   (:import [java.util UUID]
            [com.basho.riak.client.core.query Location]
            [com.basho.riak.client.core.query Namespace]))

(def set-bucket-type "set-type")

(defn- generate-random-name []
  (-> (UUID/randomUUID) .toString))

(defn- create-bucket
  [type name]
  (-> (Namespace. type name)))

(defn- create-random-bucket
  [type]
  (create-bucket type (generate-random-name)))

(defn- create-random-set
  []
  (-> (Location. (create-random-bucket set-bucket-type) (generate-random-name))))

(def test-set (create-random-set))

(deftest register-test
  (let [test (run!
              (assoc
                noop-test
                :name      "riak2-dt-set"
                :os        debian/os
                :db        db
                :client    (create-set-client test-set)
                :model     (model/set)
                :checker   (checker/compose {:html   timeline/html
                                             :linear checker/linearizable})
                :nemesis   (nemesis/partition-random-halves)
                :generator (gen/phases
                            (->> (range)
                                 (map (fnn [x] {:type "invoke"
                                              :f    :add
                                              :value x}))
                                 gen/seq
                                 (gen/stagger 1/10)
                                 (gen/delay 1)
                                 (gen/nemesis
                                  (gen/seq
                                   (cycle [(gen/sleep 30)
                                           {:type :info :f :start}
                                           (gen/sleep 200)
                                           {:type :info :f :stop}])))
                                 (gen/time-limit 400))
                            (gen/nemesis
                             (gen/once {:type :info :f :stop}))
                            (gen/clients
                             (gen/once {:type :invoke :f :read})))))]
    (is (:valid? (:results test)))
        (report/linearizability (:linear (:results test)))))
