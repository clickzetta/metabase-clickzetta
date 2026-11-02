(ns metabase.test.data.cz-presto
  "Presto JDBC driver test extensions."
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [metabase.config :as config]
   [metabase.driver :as driver]
   [metabase.driver.ddl.interface :as ddl.i]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.test.data.dataset-definitions :as defs]
   [metabase.test.data.interface :as tx]
   [metabase.test.data.sql :as sql.tx]
   [metabase.test.data.sql-jdbc :as sql-jdbc.tx]
   [metabase.test.data.sql-jdbc.execute :as execute]
   [metabase.test.data.sql-jdbc.load-data :as load-data]
   [metabase.test.data.sql.ddl :as ddl]
   [metabase.util.log :as log])
  (:import
   (java.sql Connection PreparedStatement)))

(set! *warn-on-reflection* true)

(sql-jdbc.tx/add-test-extensions! :cz-presto)

(defmethod tx/sorts-nil-first? :cz-presto [_ _] false)

;; during unit tests don't treat clickzetta as having FK support
(defmethod driver/database-supports? [:cz-presto :foreign-keys] [_driver _feature _db] (not config/is-test?))

(defmethod tx/aggregate-column-info :cz-presto
  ([driver ag-type]
   ((get-method tx/aggregate-column-info ::tx/test-extensions) driver ag-type))

  ([driver ag-type field]
   (merge
    ((get-method tx/aggregate-column-info ::tx/test-extensions) driver ag-type field)
    (when (= ag-type :sum)
      {:base_type :type/BigInteger}))))

(prefer-method tx/aggregate-column-info :cz-presto ::tx/test-extensions)

;; in the past, we had to manually update our Docker image and add a new catalog for every new dataset definition we
;; added. That's insane. Just use the `test-data` catalog and put everything in that, and use
;; `db-qualified-table-name` like everyone else.
(def ^:private test-catalog-name "example")

(doseq [[base-type db-type] {:type/BigInteger             "BIGINT"
                             :type/Boolean                "BOOLEAN"
                             :type/Date                   "DATE"
                             :type/DateTime               "TIMESTAMP"
                             :type/DateTimeWithTZ         "TIMESTAMP WITH TIME ZONE"
                             :type/DateTimeWithZoneID     "TIMESTAMP WITH TIME ZONE"
                             :type/DateTimeWithZoneOffset "TIMESTAMP WITH TIME ZONE"
                             :type/Decimal                "DECIMAL"
                             :type/Float                  "DOUBLE"
                             :type/Integer                "INTEGER"
                             :type/Text                   "VARCHAR"
                             :type/Time                   "TIME"
                             :type/TimeWithTZ             "TIME WITH TIME ZONE"}]
  (defmethod sql.tx/field-base-type->sql-type [:cz-presto base-type] [_ _] db-type))

(defn dbdef->connection-details [_database-name]
  (let [base-details
        {:instance                                (tx/db-test-env-var-or-throw :cz-presto :instance "instance")
         :service                                 (tx/db-test-env-var-or-throw :cz-presto :service "singdata.com")
         :workspace                               (tx/db-test-env-var-or-throw :cz-presto :workspace "example")
         :user                                    (tx/db-test-env-var-or-throw :cz-presto :user "user")
         :password                                (tx/db-test-env-var-or-throw :cz-presto :password "password")
         :virtualCluster                          (tx/db-test-env-var-or-throw :cz-presto :virtualcluster "default")
         :schema                                  (tx/db-test-env-var :cz-presto :schema "metabase")
         :additional                              (tx/db-test-env-var-or-throw :cz-presto :additional "transpile=127.0.0.1:8531")}]
    base-details))

(defmethod tx/dbdef->connection-details :cz-presto
  [_ _ {:keys [database-name]}]
  (dbdef->connection-details database-name))

(defmethod execute/execute-sql! :cz-presto
  [& args]
  (log/infof "execute-sql! args: %s" args)
  (apply execute/sequentially-execute-sql! args))

(defmethod load-data/chunk-size :cz-presto
  [_driver _dbdef tabledef]
  (case (:table-name tabledef)
    "people" 30
    "reviews" 40
    "orders" 30
    "venues" 50
    "products" 50
    "cities" 50
    "sightings" 50
    "incidents" 50
    "checkins" 25
    "airport" 50
    100))

(defmethod load-data/row-xform :cz-presto
  [_driver _dbdef _tabledef]
  (load-data/add-ids-xform))

(defmethod load-data/do-insert! :cz-presto
  [driver ^Connection conn table-identifier rows]
  (let [statements (ddl/insert-rows-dml-statements driver table-identifier rows)]
    (doseq [[^String sql & params] statements]
      (try
        (with-open [^PreparedStatement stmt (.prepareStatement conn sql)]
          (sql-jdbc.execute/set-parameters! driver stmt params)
          (let [[_tag _identifier-type components] table-identifier
                table-name                         (last components)
                rows-affected                      (.executeUpdate stmt)]
            (log/infof "[%s] Inserted %d rows into %s." driver rows-affected table-name)))
        (catch Throwable e
          (throw (ex-info (format "[%s] Error executing SQL: %s" driver (ex-message e))
                          {:driver driver, :sql sql, :params params}
                          e)))))))

(defmethod sql.tx/drop-db-if-exists-sql :cz-presto [_ _] nil)
(defmethod sql.tx/create-db-sql         :cz-presto [_ _] nil)

(defmethod sql.tx/qualified-name-components :cz-presto
  ;; use the default schema from the in-memory connector
  ([_ _db-name]                      [test-catalog-name "metabase"])
  ([_ db-name table-name]            [test-catalog-name "metabase" (tx/db-qualified-table-name db-name table-name)])
  ([_ db-name table-name field-name] [test-catalog-name "metabase" (tx/db-qualified-table-name db-name table-name) field-name]))

(defmethod sql.tx/pk-sql-type :cz-presto
  [_]
  "INTEGER")

(defmethod sql.tx/create-table-sql :cz-presto
  [driver dbdef tabledef]
  ;; Presto doesn't support NOT NULL columns
  (let [tabledef (update tabledef :field-definitions (fn [field-defs]
                                                       (for [field-def field-defs]
                                                         (dissoc field-def :not-null?))))
        ;; strip out the PRIMARY KEY stuff from the CREATE TABLE statement
        sql      ((get-method sql.tx/create-table-sql :sql/test-extensions) driver dbdef tabledef)]
    (str/replace sql #", PRIMARY KEY \([^)]+\)" "")))

(deftest ^:parallel create-table-sql-test
  (testing "Make sure logic to strip out NOT NULL and PRIMARY KEY stuff works as expected"
    (let [db-def    (tx/get-dataset-definition defs/test-data)
          table-def (-> db-def :table-definitions second)]
      (is (= "CREATE TABLE \"example\".\"metabase\".\"test_data_categories\" (\"id\" INTEGER, \"name\" VARCHAR) ;"
             (sql.tx/create-table-sql :cz-presto db-def table-def))))))

(defmethod ddl.i/format-name :cz-presto
  [_driver table-or-field-name]
  (str/replace table-or-field-name #"-" "_"))

;; Presto doesn't support FKs, at least not adding them via DDL
(defmethod sql.tx/add-fk-sql :cz-presto
  [_driver _dbdef _tabledef _fielddef]
  nil)
