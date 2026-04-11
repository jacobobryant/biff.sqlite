(ns com.biffweb.sqlite.impl.auth
  "SQLite-backed implementations for biff.authenticate DB store keys."
  (:require [com.biffweb.sqlite.impl.execute :as exec]))

(def auth-signin-columns
  "Column definitions for the biff-auth-signin table used by biff.authenticate."
  {:biff-auth-signin/id              {:type :uuid :primary-key true}
   :biff-auth-signin/email           {:type :text :unique true :required true}
   :biff-auth-signin/code            {:type :text :required true}
   :biff-auth-signin/created-at      {:type :inst :required true}
   :biff-auth-signin/failed-attempts {:type :int :required true}
   :biff-auth-signin/params          {:type :text}})

(def db-fns
  "Map of biff.authenticate DB store functions backed by SQLite."
  {:biff.auth/get-user-id
   (fn [ctx email]
     (:user/id (first (exec/execute ctx {:select [:user/id]
                                         :from :user
                                         :where [:= :user/email email]}))))

   :biff.auth/create-user!
   (fn [ctx {:keys [email params]}]
     (let [id (random-uuid)
           now (java.time.Instant/now)]
       (exec/execute ctx {:insert-into :user
                          :values [{:user/id id
                                    :user/email email
                                    :user/joined-at now}]})
       id))

   :biff.auth/upsert-signin!
   (fn [ctx record]
     (exec/execute ctx {:insert-into :biff-auth-signin
                        :values [(merge record {:biff-auth-signin/id (random-uuid)})]
                        :on-conflict [:biff-auth-signin/email]
                        :do-update-set [:biff-auth-signin/code
                                        :biff-auth-signin/created-at
                                        :biff-auth-signin/failed-attempts
                                        :biff-auth-signin/params]})
     nil)

   :biff.auth/get-signin
   (fn [ctx email]
     (first (exec/execute ctx {:select :*
                               :from :biff-auth-signin
                               :where [:= :biff-auth-signin/email email]})))

   :biff.auth/delete-signin!
   (fn [ctx email]
     (exec/execute ctx {:delete-from :biff-auth-signin
                        :where [:= :biff-auth-signin/email email]})
     nil)

   :biff.auth/increment-failed-attempts!
   (fn [ctx email]
     (exec/execute ctx {:update :biff-auth-signin
                        :set {:biff-auth-signin/failed-attempts
                              [:+ :biff-auth-signin/failed-attempts 1]}
                        :where [:= :biff-auth-signin/email email]})
     nil)})
