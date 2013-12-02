(ns toy-wks.core
  (:require [toy-wks.incantertest :refer :all])
  (:gen-class))

(defn -main
  [& args]
  (let [_ (println "Loading dataset...")
        cm (time (cm-all-exact))
        _ (println "Building matrix...")
        ds (time (countmap-to-dataset cm))
        _ (println "Running som...")
        [som matrix] (run-som ds)]
    (println "Cells:")
    (print-state-groups som ds)
    (plot-means som matrix))
)
