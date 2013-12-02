(ns toy-wks.incantertest
  (:require [incanter.core :refer :all]
             [incanter.stats :refer :all]
             [incanter.charts :refer :all]
             [incanter.som :refer :all]
             [incanter.io :refer :all]
             [clojure-csv.core :as csv]
             [cheshire.core :as json]
             [clojure.java.io :as io]
             )
  (:import (com.clearspring.analytics.stream StreamSummary)))


(def ^:dynamic *max-names* 20) ; max names to include in a row

(defprotocol CountMap
  (to-row [this state])
  (with-row [this [state _ _ name ct]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Structure for maintaining exact counts

(defrecord ExactCountMap []
  CountMap
  (to-row [this state] 
    (assoc 
      (->> (get this state)
        (sort-by second)
        reverse
        (take *max-names*)
        flatten
        (apply hash-map))
      "_State" state))
  (with-row [this [state _ _ name ct]] 
	  (let [ct-int (read-string ct)]
	   (cond
	     (get-in this [state name]) (update-in this [state name] #(+ % ct-int))
	     (get-in this [state]) (assoc-in this [state name] ct-int)
	     :else (assoc this state (sorted-map name ct-int)))))
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Structure for maintaining approx counts

(def ^:dynamic *capacity* 100000) ; should be > max-names... why is this so terrible?

(defrecord ApproxCountMap []
  CountMap
  (to-row [this state] 
    (if
	    (contains? this state)
	      (reduce 
	        #(assoc %1 (.getItem %2) (.getCount %2)) 
	        {"_State" state}
	        (.topK (get this state) *max-names*))
       nil))
  (with-row [this [state _ _ name ct]]
	  (let [ct-int (read-string ct)]
	   (if-let [sketch (get this state)]
	     (do (.offer sketch name ct-int) this)
	     (assoc this state
	            (doto (StreamSummary. *capacity*)
	              (.offer name ct-int))))))
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Parsing bits

(def ^:private pattern #"(\w{2}).TXT$")

(def txtfiles
 (->> "resources/namesbystate"
   io/file
   file-seq
   (filter #(re-matches pattern (.getName %)))
   ))

(defn stateinfo [file]
  (csv/parse-csv (io/reader file)))

(def sampledata
  [["AK" "M" "2012" "Marshall" "5"] 
  ["AK" "M" "2011" "Marshall" "8"]
  ["AL" "M" "2011" "Marshall" "8"]
  ["AK" "M" "2012" "Mike" "2"]])

(defn alldata [] 
  (mapcat stateinfo txtfiles))

(defn get-countmap 
  [base data]
  (reduce (fn [m r] (with-row m r)) base data))

(defn countmap-to-dataset
  [countmap]
    (let [base-rows (map #(to-row countmap %1) (keys countmap))
        cols (apply sorted-set (mapcat keys base-rows))
        zero-row (apply assoc {} (interleave cols (take (count cols) (repeatedly (constantly 0)))))
        rows (map (partial merge zero-row) base-rows)]
    (dataset cols rows)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Functions to use at the REPL

(defmacro tdef [sym & body]
  `(do (prn ~sym) (time (def ~sym ~@body))))

(defn cm-sample-approx []
  (get-countmap (ApproxCountMap.) sampledata))

(defn cm-sample-exact []
  (get-countmap (ExactCountMap.) sampledata))

; Approx runtime on my machine: 30s
(defn cm-all-approx []
  (get-countmap (ApproxCountMap.) (alldata)))

; Approx runtime on my machine: 35s
(defn cm-all-exact []
  (get-countmap (ExactCountMap.) (alldata)))

(defn setup-all []
  (let [_ (prn "all-approx...")
        xcmaa (time (cm-all-approx))
        _ (prn "all-exact...")
        xcmae (time (cm-all-exact))
        _ (prn "ds-all-approx...")
        xdsaa (time (countmap-to-dataset xcmaa))
        _ (prn "ds-all-exact...")
        xdsae (time (countmap-to-dataset xcmae))]
    (def cmaa xcmaa)
    (def cmae xcmae)
    (def dsaa xdsaa)
    (def dsae xdsae)))

(defn print-state-groups [som dataset]
  (doseq [rws (vals (:sets som))]
    (println (sel dataset :cols "_State" :rows rws))))

(defn plot-means [som matrix]
  (let [sets (:sets som)
        cell-means (map #(map mean (trans (sel matrix :rows (sets %)))) (keys sets))
        x (range (ncol matrix))]
    (loop [means cell-means
           plot (xy-plot x (first means))]
      (if (empty? means)
        (view plot)
        (recur (rest means) (doto plot (add-lines x (first means))))))))


(defn run-som [dataset]
  (let [matrix (to-matrix (sel dataset :except-cols "_State"))
        som (som-batch-train matrix :cycles 10 :alpha 0.5 :beta 3)]
    [som matrix]
    )) 
