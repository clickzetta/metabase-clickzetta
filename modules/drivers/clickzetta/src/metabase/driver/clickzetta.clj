(ns metabase.driver.clickzetta
  "ClickZetta Lakehouse Driver."
  (:require
   [buddy.core.codecs :as codecs]
   [clojure.java.jdbc :as jdbc]
   [clojure.set :as set]
   [clojure.string :as str]
   [honey.sql.helpers :as sql.helpers]
   [java-time.api :as t]
   [medley.core :as m]
   [metabase.driver :as driver]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.execute.legacy-impl :as sql-jdbc.legacy]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql.parameters.substitution
    :as sql.params.substitution]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.util :as u]
   [metabase.util.date-2 :as u.date]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.i18n :refer [trs]]
   [metabase.util.log :as log])
  (:import
   (com.clickzetta.client.jdbc.arrow.util CZTimestamp)
   (com.clickzetta.client.jdbc.core CZConnection)
   (com.mchange.v2.c3p0 C3P0ProxyConnection)
   (java.sql Connection ResultSet Time Types JDBCType PreparedStatement)
   (java.time LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime ZoneId)
   (java.time.format DateTimeFormatter)
   (java.time.temporal ChronoField Temporal)))

(set! *warn-on-reflection* true)

(driver/register! :clickzetta, :parent #{:sql-jdbc ::sql-jdbc.legacy/use-legacy-classes-for-read-and-set})

(doseq [[feature supported?] {:set-timezone                    true
                              :basic-aggregations              true
                              :standard-deviation-aggregations true
                              :expressions                     true
                              :native-parameters               true
                              :expression-aggregations         true
                              :binning                         true
                              :foreign-keys                    true
                              :now                             true}]
  (defmethod driver/database-supports? [:clickzetta feature] [_driver _feature _db] supported?))

(defmethod sql-jdbc.sync/database-type->base-type :clickzetta
  [_ base-type]
  ({:decimal                    :type/Decimal
    :int                        :type/Integer
    :bigint                     :type/BigInteger
    :smallint                   :type/Integer
    :tinyint                    :type/Integer
    :float                      :type/Float
    :double                     :type/Float
    :varchar                    :type/Text
    :char                       :type/Text
    :string                     :type/Text
    :boolean                    :type/Boolean
    :date                       :type/Date
    :timestamp_ltz              :type/DateTime
    :map                        :type/Dictionary
    :struct                     :type/*
    :array                      :type/Array
    :binary                     :type/*} base-type))

;;;----------------------------------------------------------------------------
;;; honey sql translation
;;;----------------------------------------------------------------------------
(defmethod sql.qp/honey-sql-version :clickzetta
  [_driver]
  2)

(defmethod sql.qp/current-datetime-honeysql-form :clickzetta
  [_]
  (h2x/with-database-type-info :%current_timestamp :TIMESTAMP_LTZ))

(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :seconds]
  [_ _ expr]
  (h2x/->timestamp [:from_unixtime expr]))

(defn- date-format [format-str expr]
  [:date_format expr (h2x/literal format-str)])

(defn- str-to-date [format-str expr]
  (h2x/->timestamp [:from_unixtime [:unix_timestamp expr (h2x/literal format-str)]]))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:clickzetta :default]         [_ _ expr] (h2x/->timestamp expr))
(defmethod sql.qp/date [:clickzetta :minute]          [_ _ expr] (trunc-with-format "yyyy-MM-dd HH:mm" (h2x/->timestamp expr)))
(defmethod sql.qp/date [:clickzetta :minute-of-hour]  [_ _ expr] [:minute (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :hour]            [_ _ expr] (trunc-with-format "yyyy-MM-dd HH" (h2x/->timestamp expr)))
(defmethod sql.qp/date [:clickzetta :hour-of-day]     [_ _ expr] [:hour (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :day]             [_ _ expr] (trunc-with-format "yyyy-MM-dd" (h2x/->timestamp expr)))
(defmethod sql.qp/date [:clickzetta :day-of-month]    [_ _ expr] [:dayofmonth (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :day-of-year]     [_ _ expr] (h2x/->integer (date-format "D" (h2x/->timestamp expr))))
(defmethod sql.qp/date [:clickzetta :month]           [_ _ expr] [:date_trunc (h2x/literal :MM) (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :month-of-year]   [_ _ expr] [:month (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :quarter-of-year] [_ _ expr] [:quarter (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :year]            [_ _ expr] [:date_trunc (h2x/literal :year) (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :week]            [_ _ expr] [:date_trunc (h2x/literal :week) (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta ::quarter]        [_ _ expr] [:date_trunc (h2x/literal ::quarter) (h2x/->timestamp expr)])

(defmethod sql.qp/date [:clickzetta :day-of-week]
  [_driver _unit expr]
  [:dayofweek (h2x/->timestamp expr)])

(defmethod sql.qp/date [:clickzetta :week-of-year-iso]
  [_driver _unit expr]
  [:weekofyear (h2x/->timestamp expr)])

(defmethod sql.qp/->honeysql [:clickzetta :replace]
  [driver [_ arg pattern replacement]]
  [:regexp_replace
   (sql.qp/->honeysql driver arg)
   (sql.qp/->honeysql driver pattern)
   (sql.qp/->honeysql driver replacement)])

(defmethod sql.qp/->honeysql [:clickzetta :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern) [:raw (int 0)]])

(defmethod sql.qp/->honeysql [:clickzetta :median]
  [driver [_ arg]]
  [:percentile (sql.qp/->honeysql driver arg) [:raw (float 0.5)]])

(defmethod sql.qp/->honeysql [:clickzetta :percentile]
  [driver [_ arg p]]
  [:percentile (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)])

(defmethod sql.qp/add-interval-honeysql-form :clickzetta
  [_ hsql-form amount unit]
  [:dateadd
   [:raw (name unit)]
   [:raw (int amount)]
   (h2x/->timestamp hsql-form)])

(defmethod sql.qp/datetime-diff [:clickzetta :year]
  [_driver _unit x y]
  [:timestampdiff [:raw "year"] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :quarter]
  [_driver _unit x y]
  [:timestampdiff [:raw "quarter"] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :month]
  [_driver _unit x y]
  [:timestampdiff [:raw "month"] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :week]
  [_driver _unit x y]
  [:timestampdiff [:raw "week"] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :day]
  [_driver _unit x y]
  [:timestampdiff [:raw "day"] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :hour]
  [_driver _unit x y]
  [:timestampdiff [:raw "hour"] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :minute]
  [_driver _unit x y]
  [:timestampdiff [:raw "minute"] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :second]
  [_driver _unit x y]
  [:timestampdiff [:raw "second"] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :millisecond]
  [_driver _unit x y]
  [:timestampdiff [:raw "millisecond"] x y])

(defmethod sql.qp/quote-style :clickzetta
  [_driver]
  :mysql)

(defmethod sql.qp/->honeysql [:clickzetta :power]
  [driver [_ arg power]]
  [:pow
   (sql.qp/->honeysql driver arg)
   (sql.qp/->honeysql driver power)])

(defmethod sql.qp/->honeysql [:clickzetta :log]
  [driver [_ arg]]
  [:log10
   (sql.qp/->honeysql driver arg)])

(defmethod sql.qp/->honeysql [:clickzetta :convert-timezone]
  [driver [_ arg target-timezone source-timezone]]
  [:convert_timezone
   source-timezone
   target-timezone
   (sql.qp/->honeysql driver arg)])

(def ^:private ^:const timestamp-with-time-zone-db-type "timestamp")

(defmethod sql.qp/->honeysql [:clickzetta ZonedDateTime]
  [_ ^ZonedDateTime t]
  (h2x/cast timestamp-with-time-zone-db-type (u.date/format-sql t)))

(defmethod sql.qp/->honeysql [:clickzetta OffsetDateTime]
  [_ ^OffsetDateTime t]
  (h2x/cast timestamp-with-time-zone-db-type (u.date/format-sql t)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Connectivity                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+
(defmethod driver/humanize-connection-error-message :clickzetta
  [_ message]
  (log/spy :error (type message))
  (condp re-matches message
    #"(?s).*Object does not exist.*$"
    :database-name-incorrect

    message))

(defmethod sql-jdbc.conn/data-warehouse-connection-pool-properties :clickzetta
  [driver database]
  (merge
   ((get-method sql-jdbc.conn/data-warehouse-connection-pool-properties :sql-jdbc) driver database)
   {"testConnectionOnCheckout" false
    "testConnectionOnCheckin" false}))

(defmethod sql-jdbc.conn/connection-details->spec :clickzetta
  [_ {:keys [user password virtualCluster schema instance service workspace additional]}]
  (let [encoded-password (java.net.URLEncoder/encode (str password) "UTF-8")
        additional-options-base (str "user=" user "&password=" encoded-password "&virtualCluster=" virtualCluster)
        additional-options (if (seq schema) (str additional-options-base "&schema=" schema) additional-options-base)
        additional-options-with-additional-params (if (seq additional) (str additional-options "&" additional) additional-options)]
    (sql-jdbc.common/handle-additional-options {:classname                     "com.clickzetta.client.jdbc.ClickZettaDriver"
                                                :subprotocol                   "clickzetta"
                                                :subname                       (str "//" instance "." service "/" workspace)}
                                               {:additional-options additional-options-with-additional-params})))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                      Sync                                                      |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- describe-schema [driver conn workspace schema]
  (log/info "Get tables of schema : " schema)
  (when (not-empty schema)
    (let [sql (str "SHOW TABLES IN " schema)]
      (log/info "Get tables by : " sql)
      (into #{} (map (fn [{table :table_name}]
                       {:name        table
                        :schema      schema}))
            (jdbc/reducible-query {:connection conn} sql)))))

(def ^:private excluded-schemas
  #{"information_schema"})

(defn- all-schemas [driver conn workspace]
  (let [sql "SHOW SCHEMAS"]
    (log/info "Get all schemas: " sql)
    (into []
          (map (fn [{:keys [schema_name]}]
                 (when-not (contains? excluded-schemas schema_name)
                   (describe-schema driver conn workspace schema_name))))
          (jdbc/reducible-query {:connection conn} sql))))

(defmethod driver/describe-database* :clickzetta
  [driver database]
  (let [{:keys [workspace schema]} (:details database)]
    (sql-jdbc.execute/do-with-connection-with-options
     driver
     database
     nil
     (fn [^Connection conn]
       (let [schemas (if schema #{(describe-schema driver conn workspace schema)}
                         (all-schemas driver conn workspace))]
         (log/info "Get all schemas done: " schemas)
         {:tables (reduce set/union #{} schemas)})))))

(defn- valid-describe-table-row? [{:keys [column_name data_type]}]
  (every? (every-pred (complement str/blank?)
                      (complement #(str/starts-with? % "#")))
          [column_name data_type]))

(defn- dash-to-underscore [s]
  (when s
    (str/replace s #"\"" "`")))

(defmethod driver/describe-table :clickzetta
  [driver database {table-name :name, schema :schema}]
  {:name   table-name
   :schema schema
   :fields
   (sql-jdbc.execute/do-with-connection-with-options
    driver
    database
    nil
    (fn [^Connection conn]
      (let [results (jdbc/query {:connection conn} [(format
                                                     "show columns in %s.%s"
                                                     (dash-to-underscore schema)
                                                     (dash-to-underscore table-name))])]
        (set
         (for [[idx {col-name :column_name, data-type :data_type, :as result}] (m/indexed results)
               :when (valid-describe-table-row? result)]
           {:name              col-name
            :database-type     (first (str/split (first (str/split data-type #" ")) #"\("))
            :base-type         (sql-jdbc.sync/database-type->base-type :clickzetta (keyword (first (str/split (first (str/split (first (str/split data-type #" ")) #"\(")) #"\<"))))
            :database-position idx})))))})

(defmethod driver/describe-table-fks :clickzetta
  [_driver _database _table]
  nil)

(defmethod driver/can-connect? :clickzetta
  [_driver _details]
  true)

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                            sql-jdbc implementations                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.execute/prepared-statement :clickzetta
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn
                                sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (try
        (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
        (catch Throwable e
          (log/debug e (trs "Error setting prepared statement fetch direction to FETCH_FORWARD"))))
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))

(defmethod sql-jdbc.execute/statement :clickzetta
  [_ ^Connection conn]
  (let [stmt (.createStatement conn
                               ResultSet/TYPE_FORWARD_ONLY
                               ResultSet/CONCUR_READ_ONLY)]
    (try
      (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
      (catch Throwable e
        (log/debug e (trs "Error setting statement fetch direction to FETCH_FORWARD"))))
    stmt))

(defn- pooled-conn->cz-conn
  ^CZConnection [^C3P0ProxyConnection pooled-conn]
  (.unwrap pooled-conn CZConnection))

(def ^:dynamic ^:private *original-connection-spec* nil)

(defn- set-connection-options! [driver ^java.sql.Connection conn {:keys [^String session-timezone], :as _options}]
  (let [underlying-conn (pooled-conn->cz-conn conn)]
    (sql-jdbc.execute/set-best-transaction-level! driver conn)
    (when-not (str/blank? session-timezone)
      (.setConfig underlying-conn "cz.sql.timezone" session-timezone))))

(defmethod sql-jdbc.execute/do-with-connection-with-options :clickzetta
  [driver db-or-id-or-spec options f]
  (cond
    (nil? *original-connection-spec*)
    (binding [*original-connection-spec* db-or-id-or-spec]
      (sql-jdbc.execute/do-with-connection-with-options driver db-or-id-or-spec options f))

    :else
    (sql-jdbc.execute/do-with-resolved-connection
     driver
     db-or-id-or-spec
     (dissoc options :session-timezone)
     (fn [^java.sql.Connection conn]
       (when-not (sql-jdbc.execute/recursive-connection?)
         (set-connection-options! driver conn options))
       (f conn)))))

(defn- date-time->substitution [ts-str]
  (sql.params.substitution/make-stmt-subs "timestamp ?" [ts-str]))

(defmethod sql.params.substitution/->prepared-substitution [:clickzetta ZonedDateTime]
  [_ ^ZonedDateTime t]
  (let [report-zone       (driver-api/report-timezone-id-if-supported :clickzetta (driver-api/database (driver-api/metadata-provider)))
        ^ZonedDateTime ts (if (str/blank? report-zone) t (t/with-zone-same-instant t (t/zone-id report-zone)))]
    (date-time->substitution (.format ts DateTimeFormatter/ISO_OFFSET_DATE_TIME))))

(defmethod sql.params.substitution/->prepared-substitution [:clickzetta LocalDateTime]
  [_ ^LocalDateTime t]
  (date-time->substitution (.format t DateTimeFormatter/ISO_LOCAL_DATE_TIME)))

(defmethod sql.params.substitution/->prepared-substitution [:clickzetta OffsetDateTime]
  [_ ^OffsetDateTime t]
  (date-time->substitution (.format t DateTimeFormatter/ISO_OFFSET_DATE_TIME)))

(defn- set-time-param
  [^PreparedStatement ps ^Integer i ^Temporal t]
  (let [millis-of-day (.get t ChronoField/MILLI_OF_DAY)]
    (.setTime ps i (Time. millis-of-day))))

(defmethod sql-jdbc.execute/set-parameter [:clickzetta OffsetTime]
  [_ ^PreparedStatement ps ^Integer i t]
  (let [adjusted-tz (t/with-offset-same-instant t (t/zone-offset 0))]
    (set-time-param ps i adjusted-tz)))

(defmethod sql-jdbc.execute/set-parameter [:clickzetta LocalTime]
  [_ ^PreparedStatement ps ^Integer i t]
  (set-time-param ps i t))

(defn- set-object
  ([^PreparedStatement prepared-statement, ^Integer index, object]
   (log/tracef "(set-object prepared-statement %d ^%s %s)" index (some-> object class .getName) (pr-str object))
   (.setObject prepared-statement index object))

  ([^PreparedStatement prepared-statement, ^Integer index, object, ^Integer target-sql-type]
   (log/tracef "(set-object prepared-statement %d ^%s %s java.sql.Types/%s)" index (some-> object class .getName)
               (pr-str object) (.getName (JDBCType/valueOf target-sql-type)))
   (.setObject prepared-statement index object target-sql-type)))

(defmethod sql-jdbc.execute/set-parameter [:clickzetta String]
  [_ prepared-statement i t]
  (let [new-t (str/replace t "'" "\\'")]
    (set-object prepared-statement i new-t)))

;; set query_tag and remark for query
(defmethod driver/execute-reducible-query :clickzetta
  [driver {{sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [database (driver-api/database (driver-api/metadata-provider))]
    (let [schema_select (str "`" (get-in database [:details :schema] "public") "`.")]
      (let [replace_sql (str/replace sql schema_select "")]
        (let [common-name (:common_name @@(requiring-resolve 'metabase.api.common/*current-user*))
              query-hash (some-> outer-query :info :query-hash codecs/bytes->hex)
              query-tag-str (format "set query_tag='%s|%s';" common-name query-hash)
              remark (driver-api/query->remark driver outer-query)
              real-query (str query-tag-str "\n-- " remark "\n" replace_sql)]
          (let [inner-query (assoc inner-query
                                   :query  real-query
                                   :max-rows (driver-api/query->max-rows-limit outer-query))
                query       (assoc outer-query :native inner-query)]
            ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond)))))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickzetta Types/TIMESTAMP]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (let [instant (.toInstant t)
            report-zone (driver-api/report-timezone-id-if-supported :clickzetta (driver-api/database (driver-api/metadata-provider)))
            zone-id (ZoneId/of (or report-zone "UTC"))
            zoned-datetime (.atZone instant zone-id)]
        (.toLocalDateTime zoned-datetime)))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickzetta Types/TIMESTAMP_WITH_TIMEZONE]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (let [instant (.toInstant t)
            report-zone (driver-api/report-timezone-id-if-supported :clickzetta (driver-api/database (driver-api/metadata-provider)))
            zone-id (ZoneId/of (or report-zone "UTC"))
            zoned-datetime (.atZone instant zone-id)]
        (.toLocalDateTime zoned-datetime)))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           Other Driver Method Impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(prefer-method driver/database-supports? [:clickzetta :set-timezone] [:sql-jdbc :set-timezone])
