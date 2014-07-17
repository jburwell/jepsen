(ns jepsen.system.riak2
   (:require [clojure.tools.logging     :refer [info]]
             [jepsen.client             :as client]
             [jepsen.db                 :as db])
   (:import [com.basho.riak.client.core RiakCluster$Builder]
            [com.basho.riak.client.core RiakNode$Builder]))

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

(defn- create-riak-client
    [host]
    (let [riak_node (.. (RiakNode$Builder.) (withRemoteAddress host) build)]
        (.. (RiakCluster$Builder. riak_node) build)))

(defrecord CreateSetClient [client]
  client/Client
  (setup! [this test host]
    (info "Setting up a client for node " host)
    (CreateSetClient. (create-riak-client host)))
  
  (invoke! [this test op]
    (case (:f op)
      :add ()
      :read ()))
  
  (teardown! [_ test]
    (.close client)))

(defn create-set-client []
  (CreateSetClient. nil))
