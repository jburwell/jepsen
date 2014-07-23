(ns jepsen.system.riak2
   (:require [clojure.tools.logging     :refer [info]]
             [jepsen.client             :as client]
             [jepsen.db                 :as db])
   (:import [java.util.concurrent ExecutionException]
            [com.basho.riak.client.api RiakClient]
            [com.basho.riak.client.api.commands FetchSet$Builder]
            [com.basho.riak.client.api.commands UpdateSet$Builder]
            [com.basho.riak.client.api.commands.datatypes SetUpdate]
            [com.basho.riak.client.core.util BinaryValue]))

(def db
  (reify db/DB
 ;   (init!
 ;     (when-not (file? ansible-riak)
 ;       (c/su
 ;           (c/exec :git :clone (str https://github.com/basho/ansible-riak.git))
 ;        )
 ;    )
    (setup! [_ test node]
      ; Do nothing -- Riak will be configured external to Jepsen
      ; TODO Beat Jepsen provisioning into submission / grok the Datomic approach
    )
    (teardown! [_ test node]
      ; Do nothing -- Riak will be provisiong occured external to Jepsen operation
      ; TODO Beat Jepsen provisioning into submission / grok the Datomic approach
    )
  )
)

(defn- symbol-to-str
  [symbol]
  (apply str (rest (str symbol))))

(defn- start-riak-client
    [host]
    (RiakClient/newClient (cons host ())))

(defrecord CreateSetClient [client set]
  client/Client
  (setup! [this test host]
    (info "Setting up a client for node " host)
    (let [riak-client (start-riak-client (symbol-to-str host))]
      (CreateSetClient. riak-client set)))
    
  (invoke! [this test op]
    (try
        (case (:f op)
            :add ((let [set-update-command (SetUpdate. )]
                   (.add set-update-command ':value)
                     (.execute client 
                        (-> (UpdateSet$Builder. set set-update-command) .build)))
                     (assoc op :type :ok))
            :read ((let [set-fetch-command (-> (FetchSet$Builder. set) .build)]
                   (-> (client)
                       (.execute set-fetch-command)
                       (.getDataType)
                       (.view)))
                   (assoc op :type :ok)))
        (catch ExecutionException e
          (assoc op :type :fail :value (-> (e) .getCause)))
        (catch InterruptedException e
          (assoc op :type :fail :value e))))
  
  (teardown! [_ test]
    (.close client)))

(defn create-set-client [set]
  (CreateSetClient. nil, set))
