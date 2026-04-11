(ns com.biffweb.sqlite.impl.auth
  "SQLite-backed implementations for biff.authenticate DB store keys.
   Does NOT depend on com.biffweb.sqlite to avoid cyclic load dependency.
   Instead, make-db-fns accepts an execute-fn parameter.")

(def auth-signin-columns
  "Column definitions for the biff-auth-signin table used by biff.authenticate."
  {:biff-auth-signin/id              {:type :uuid :primary-key true}
   :biff-auth-signin/email           {:type :text :unique true :required true}
   :biff-auth-signin/code            {:type :text :required true}
   :biff-auth-signin/created-at      {:type :inst :required true}
   :biff-auth-signin/failed-attempts {:type :int}
   :biff-auth-signin/params          {:type :text}})

(defn make-db-fns
  "Creates a map of biff.authenticate DB store functions backed by SQLite.
   execute-fn should be a function of [ctx input] that executes SQL."
  [execute-fn]
  {:biff.auth/get-user-id
   (fn [ctx email]
     (:user/id (first (execute-fn ctx {:select [:user/id]
                                       :from :user
                                       :where [:= :user/email email]}))))

   :biff.auth/create-user!
   (fn [ctx {:keys [email params]}]
     (let [id (random-uuid)
           now (java.time.Instant/now)]
       (execute-fn ctx {:insert-into :user
                        :values [{:user/id id
                                  :user/email email
                                  :user/joined-at now}]})
       id))

   :biff.auth/upsert-signin!
   (fn [ctx record]
     (let [record (merge {:biff-auth-signin/failed-attempts 0} record)]
       (execute-fn ctx {:insert-into :biff-auth-signin
                        :values [{:biff-auth-signin/id (random-uuid)
                                  :biff-auth-signin/email (:biff-auth-signin/email record)
                                  :biff-auth-signin/code (:biff-auth-signin/code record)
                                  :biff-auth-signin/created-at (:biff-auth-signin/created-at record)
                                  :biff-auth-signin/failed-attempts (:biff-auth-signin/failed-attempts record)
                                  :biff-auth-signin/params (:biff-auth-signin/params record)}]
                        :on-conflict [:biff-auth-signin/email]
                        :do-update-set [:biff-auth-signin/code
                                        :biff-auth-signin/created-at
                                        :biff-auth-signin/failed-attempts
                                        :biff-auth-signin/params]})
       nil))

   :biff.auth/get-signin
   (fn [ctx email]
     (first (execute-fn ctx {:select :*
                             :from :biff-auth-signin
                             :where [:= :biff-auth-signin/email email]})))

   :biff.auth/delete-signin!
   (fn [ctx email]
     (execute-fn ctx {:delete-from :biff-auth-signin
                      :where [:= :biff-auth-signin/email email]})
     nil)

   :biff.auth/increment-failed-attempts!
   (fn [ctx email]
     (execute-fn ctx {:update :biff-auth-signin
                      :set {:biff-auth-signin/failed-attempts
                            [:+ :biff-auth-signin/failed-attempts 1]}
                      :where [:= :biff-auth-signin/email email]})
     nil)})
