(ns jepsen.system.riak2
   (:require [clojure.tools.logging     :refer [info]]
             [jepsen.client             :as client]
             [jepsen.db                 :as db])
   (:import [java.nio.charset Charset]
            [java.util UUID]
            [com.basho.riak.client.api RiakClient]
            [com.basho.riak.client.core.query Namespace]))

(def set-bucket-type "set-type")

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

(defn- create-bucket
  [type name]
  (-> (Namespace. type name)))

(defn- create-random-bucket
  [type]
  (create-bucket type (-> (UUID/randomUUID) .toString)))

(defn- start-riak-client
    [host]
    (RiakClient/newClient (cons host)))

(defrecord CreateSetClient [client bucket]
  client/Client
  (setup! [this test host]
    (info "Setting up a client for node " host)
    (let [riak-client (start-riak-client (symbol-to-str host))]
      (CreateSetClient. riak-client bucket)))
    
  (invoke! [this test op]
    (case (:f op)
      :add ()
      :read ()))
  
  (teardown! [_ test]
    (.close client)))

(defn create-set-client []
    (let [bucket (create-random-bucket set-bucket-type)]
        (CreateSetClient. nil, bucket)))
