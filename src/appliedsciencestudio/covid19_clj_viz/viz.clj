(ns appliedsciencestudio.covid19-clj-viz.viz
  (:require [appliedsciencestudio.covid19-clj-viz.china :as china]
            [appliedsciencestudio.covid19-clj-viz.deutschland :as deutschland]
            [appliedsciencestudio.covid19-clj-viz.italia :as italia]
            [appliedsciencestudio.covid19-clj-viz.johns-hopkins :as jh]
            [appliedsciencestudio.covid19-clj-viz.world-bank :as world-bank]
            [clojure.set :as set :refer [rename-keys]]
            [clojure.string :as string]
            [jsonista.core :as json]
            [oz.core :as oz]))

(oz/start-server! 8082)

;; Minimum viable geographic visualization
(oz/view! {:data {:url "/public/data/deutschland-bundeslaender.geo.json"
                  :format {:type "json"
                           :property "features"}}
           :mark "geoshape"})

;; Now some setup for more interesting visualizations
(def applied-science-palette
  {:pink   "#D46BC8"
   :green  "#38D996"
   :blue   "#4FADFF"
   :purple "#9085DA"})

(def deutschland-geojson-with-data
  (update (json/read-value (java.io.File. "resources/public/public/data/deutschland-bundeslaender-original.geo.json")
                           (json/object-mapper {:decode-key-fn true}))
          :features
          (fn [features]
            (mapv (fn [feature]
                    (assoc feature
                           :Bundesland     (:NAME_1 (:properties feature))
                           :Cases          (get-in deutschland/bundeslaender-data [(:NAME_1 (:properties feature)) :cases] 0)
                           :Cases-per-100k (get-in deutschland/bundeslaender-data [(:NAME_1 (:properties feature)) :cases-per-100k] 0)))
                  features))))



(comment

  ;;;; Create new GeoJSONs with COVID19 data added

  ;; Germany
  ;; medium quality GeoJSON from https://github.com/isellsoap/deutschlandGeoJSON/blob/master/2_bundeslaender/3_mittel.geojson
  (json/write-value (java.io.File. "resources/public/public/data/deutschland-bundeslaender.geo.json")
                    deutschland-geojson-with-data)

  ;; China
  (->> (update (json/read-value (java.io.File. "resources/public/public/data/china-provinces-original.geo.json")
                                (json/object-mapper {:decode-key-fn true}))
               :features
               (fn [features]
                 (map (fn [feature]
                        (let [cases (second (first (filter (comp #{(:province (:properties feature))} first)
                                                           china/cases)))
                              pop (get china/province-populations (:province (:properties feature)))
                              cases-per-100k (double (/ cases (/ pop 100000)))]
                          (assoc feature
                                 :province (-> feature :properties :province)
                                 :cases cases
                                 :population pop
                                 :cases-binned (cond
                                                 (> cases 1000) 1000
                                                 (> cases 500)  500
                                                 (> cases 200)  200
                                                 (> cases 100)  100
                                                 :else          0)
                                 :cases-per-100k cases-per-100k)))
                      features)))
       (json/write-value (java.io.File. "resources/public/public/data/china-provinces.geo.json")))

  )

(def oz-config
  "Default settings for Oz visualizations"
  (let [font "IBM Plex Mono"]
    {:config {:style {:cell {:stroke "transparent"}}
              :legend {:labelFont font
                       :labelFontSize 12
                       :titleFont "IBM Plex Mono"
                       :gradientThickness 40}
              :axis {:labelFont font
                     :titleFont font
                     :titleFontSize 20}}
     :title {:font "IBM Plex Sans"
             :fontSize 14
             :anchor "middle"}}))

(def european-state-dimensions
  {:width 550 :height 700})

;;;; ===========================================================================
;;;; Show the Italian map
;;;  By region

(def italia-region-geojson-with-data
  (update (json/read-value (java.io.File. "resources/public/public/data/limits_IT_regions-original.geo.json")
                           (json/object-mapper {:decode-key-fn true}))
          :features
          (fn [features]
            (mapv (fn [feature]
                    (assoc feature
                           :reg_name     (:reg_name (:properties feature))
                           :Cases          (get-in italia/region-data [(:reg_name (:properties feature)) :cases] 0)
                           :Cases-per-100k (get-in italia/region-data [(:reg_name (:properties feature)) :cases-per-100k] 0)))
                  features))))

(oz/view!
 (merge-with merge oz-config european-state-dimensions
             {:title {:text "COVID19 cases in Italy, by region, per 100k inhabitants"}
              :data {:name "italy"
                     :values italia-region-geojson-with-data
                     :format {:property "features"}},
              :mark {:type "geoshape" :stroke "white" :strokeWidth 1}
              :encoding {:color {:field "Cases-per-100k",
                                 :type "quantitative"
                                 :scale {:domain [0
                                                  ;; NB: compare Hubei's 111 to German maximum. (It was 0.5 when I started this project, and ~1 now.)
                                                  (apply max (map :cases-per-100k (vals italia/region-data)))]}}
                         :tooltip [{:field "reg_name" :type "nominal"}
                                   {:field "Cases" :type "quantitative"}]}
              :selection {:highlight {:on "mouseover" :type "single"}}}))

;;;; ===========================================================================
;;;; Show the Italian map
;;;  By province

(def italia-prov-geojson-with-data
  (update (json/read-value (java.io.File. "resources/public/public/data/limits_IT_provinces-original.geo.json")
                           (json/object-mapper {:decode-key-fn true}))
          :features
          (fn [features]
            (mapv (fn [feature]
                    (assoc feature
                           :prov_name     (:prov_name (:properties feature))
                           :Cases          (get-in italia/province-data [(:prov_name (:properties feature)) :cases] 0)
                           :Cases-per-100k (get-in italia/province-data [(:prov_name (:properties feature)) :cases-per-100k] 0)))
                  features))))

(oz/view!
 (merge-with merge oz-config european-state-dimensions
             {:title {:text "COVID19 cases in Italy, by province, per 100k inhabitants"}
              :data {:name "italy"
                     ;; FIXME this keeps getting cached somewhere in Firefox or Oz
                     ;; :url "/public/data/deutschland-bundeslaender.geo.json",
                     :values italia-prov-geojson-with-data
                     :format {:property "features"}},
              :mark {:type "geoshape" :stroke "white" :strokeWidth 1}
              :encoding {:color {:field "Cases-per-100k",
                                 :type "quantitative"
                                 :scale {:domain [0
                                                  ;; NB: compare Hubei's 111 to German maximum. (It was 0.5 when I started this project, and ~1 now.)
                                                  (apply max (map :cases-per-100k (vals italia/province-data)))]}}
                         :tooltip [{:field "prov_name" :type "nominal"}
                                   {:field "Cases" :type "quantitative"}]}
              :selection {:highlight {:on "mouseover" :type "single"}}}))


;;;; ===========================================================================
;;;; Geographic visualization of cases in each Germany state, shaded proportional to population
(oz/view!
 (merge-with merge oz-config european-state-dimensions
             {:title {:text "COVID19 cases in Germany, by state, per 100k inhabitants"}
              :data {:name "germany"
                     ;; FIXME this keeps getting cached somewhere in Firefox or Oz
                     ;; :url "/public/data/deutschland-bundeslaender.geo.json",
                     :values deutschland-geojson-with-data
                     :format {:property "features"}},
              :mark {:type "geoshape" :stroke "white" :strokeWidth 1}
              :encoding {:color {:field "Cases-per-100k",
                                 :type "quantitative"
                                 :scale {:domain [0
                                                  ;; NB: compare Hubei's 111 to German maximum. (It was 0.5 when I started this project, and ~1 now.)
                                                  (apply max (map :cases-per-100k (vals deutschland/bundeslaender-data)))]}}
                         :tooltip [{:field "Bundesland" :type "nominal"}
                                   {:field "Cases" :type "quantitative"}]}
              :selection {:highlight {:on "mouseover" :type "single"}}}))

;;;; ===========================================================================
;;;; Deceptive version of that same map
;;    - red has emotional valence ["#fde5d9" "#a41e23"]
;;    - we report cases without taking population into account
(oz/view! (merge oz-config european-state-dimensions
                 {:title "COVID19 cases in Germany (*not* population-scaled)"
                  :data {:name "germany"
                         ;; FIXME this keeps getting cached somewhere in Firefox or Oz
                         ;; :url "/public/data/deutschland-bundeslaender.geo.json",
                         :values deutschland-geojson-with-data
                         :format {:property "features"}},
                  :mark {:type "geoshape"  :stroke "white" :strokeWidth 1}
                  :encoding {:color {:field "Cases",
                                     :type "quantitative"
                                     :scale { ;; from https://www.esri.com/arcgis-blog/products/product/mapping/mapping-coronavirus-responsibly/
                                             :range ["#fde5d9" "#a41e23"]}}
                             :tooltip [{:field "Bundesland" :type "nominal"}
                                       {:field "Cases" :type "quantitative"}]}
                  :selection {:highlight {:on "mouseover" :type "single"}}}))


;;;; ===========================================================================
;;;; Bar chart with German states, all of Germany, and Chinese provinces

(def barchart-dimensions
  {:width 510 :height 800})

;; Bar chart of the severity of the outbreak across regions in China and Germany
;; (with or without the outlier that is China's Hubei province)
(oz/view!
 (merge oz-config
        {:title "Confirmed COVID19 cases in China and Germany",
         :data {:values (let [date "2020-03-19"]
                          (->> jh/covid19-confirmed-csv
                               ;; Notice improved readability from working with seq of maps:
                               (map #(select-keys % [:province-state :country-region date]))
                               (filter (comp #{"China" "Mainland China" "Germany"} :country-region))
                               (reduce (fn [acc m]
                                         (conj acc {:state-province (if (string/blank? (:province-state m))
                                                                      "(All German federal states)"
                                                                      (:province-state m))
                                                    :cases (get m date)}))
                                       [])
                               (concat (->> deutschland/bundeslaender-data
                                            vals
                                            (remove (comp #{"Gesamt"} :bundesland))
                                            (map (comp #(select-keys % [:state-province :cases])
                                                       #(rename-keys % {:bundesland :state-province})))
                                            (sort-by :state-province)))
                               ;; ;; FIXME this is the line to toggle:
                               (remove (comp #{"Hubei"} :state-province))
                               (sort-by :cases)))},
         :mark {:type "bar" :color "#9085DA"}
         :encoding {:x {:field "cases", :type "quantitative"}
                    :y {:field "state-province", :type "ordinal"
                        :sort nil}}}))


;;;; ===========================================================================
;;;; Bar chart of cases in Europe (FIXME scaled to World Bank population estimate)
#_(oz/view! (merge oz-config barchart-dimensions
                 {:title "COVID19 cases in European countries, per 100k inhabitants",
                  :data {:values (->> jh/covid19-confirmed-csv
                                      (map #(select-keys % [:country-region jh/last-reported-date]))
                                      (filter (comp #{"France" "Spain" "Germany"
                                                      "Sweden" "Italy" "Switzerland"
                                                      "Finland" "Greece" "UK" "Russia"
                                                      "Belgium" "Croatia" "Austria"
                                                      "North Macedonia" "Norway" "Romania"
                                                      "Denmark" "Netherlands" "Lithuania"
                                                      "Ireland" "Czech Republic" "Portugal"
                                                      "Ukraine"}
                                                    :country-region))
                                      ;; TODO (map #(assoc % :cases-per-100k))
                                      (sort-by #(get % jh/last-reported-date)))},
                  :mark {:type "bar" :color "#9085DA"}
                  :encoding {:x {:field jh/last-reported-date :type "quantitative"}
                             :y {:field "country-region", :type "ordinal" #_ "nominal"}}}))

(def china-dimensions
  {:width 570 :height 450})


;;;; ===========================================================================
;;;; Useless China chloropleth
(oz/view! (merge oz-config china-dimensions
                 {:data {:name "map"
                         :url "/public/data/china-provinces.geo.json"
                         :format {:property "features"}},
                  :mark {:type "geoshape" :stroke "white" :strokeWidth 1}
                  :encoding {:color {:field "cases-per-100k",
                                     :type "quantitative"}
                             :tooltip [{:field "province" :type "nominal"}
                                       {:field "cases" :type "quantitative"}]}}))


;;;; ===========================================================================
;;;; Deceptive China map
;; As above, but:
;;  - binned, which varies the map
;;  - not scaled to population, which assists that variation
;;  - uses red, which has inappropriate emotional valence
;; This is more visually appealing and _feels_ useful, but is actually quite deceptive.
;; (inspired by https://www.esri.com/arcgis-blog/products/product/mapping/mapping-coronavirus-responsibly/ )
(oz/view! (merge oz-config china-dimensions
                 {:title "COVID19 cases in China"
                  :data {:name "map"
                         :url "/public/data/china-provinces.geo.json",
                         :format {:property "features"}},
                  :mark {:type "geoshape" :stroke "white" :strokeWidth 1}
                  :encoding {:color {:field "cases-binned",
                                     :bin true
                                     :type "quantitative"
                                     :scale {:range ["#fde5d9"
                                                     "#f9af91"
                                                     "#f26a4d"
                                                     "#e22b26"
                                                     "#a41e23"]}}
                             :tooltip [{:field "province" :type "nominal"}
                                       {:field "cases" :type "quantitative"}
                                       {:field "cases-per-100k" :type "quantitative"}]}}))


;;;; ===========================================================================
;;;; "Best we can do" China map
;; We return to the original color scheme, drop the binning and the
;; scary red color scheme, and (most importantly) use a log scale.
;; This requires the viewer to understand orders of magnitude, which
;; might be appropriate for data science but inappropriate for
;; journalism.
(oz/view! (merge oz-config china-dimensions
                 {:title "COVID19 cases in China per 100k inhabitants, log-scaled"
                  :data {:name "map"
                         :url "/public/data/china-provinces.geo.json",
                         :format {:property "features"}},
                  :mark {:type "geoshape" :stroke "white" :strokeWidth 1}
                  :encoding {:color {:field "cases-per-100k",
                                     :scale {:type "log"}
                                     :type "quantitative"}
                             :tooltip [{:field "province" :type "nominal"}
                                       {:field "cases" :type "quantitative"}
                                       {:field "cases-per-100k" :type "quantitative"}]}}))

;; Beyond this we rely on charts.


;;;; ===========================================================================
;;;; Total Cases of Coronavirus Outside of China
;; from Chart 9 https://medium.com/@tomaspueyo/coronavirus-act-today-or-people-will-die-f4d3d9cd99ca

(oz/view!
 (merge-with merge oz-config
             {:title {:text "Total Cases of Coronavirus Outside of China"
                      :subtitle "(Countries with >50 cases as of 11.3.2020)"}
              :width 1200 :height 700
              :data {:values
                     (->> (map (fn [ctry] [ctry (zipmap jh/csv-dates (jh/country-totals ctry jh/covid19-confirmed-csv))])
                               (set/difference jh/countries #{"Mainland China" "China" "Others"} ))
                          (reduce (fn [vega-values [country date->cases]]
                                    (if (> 500 (apply max (vals date->cases))) ; ignore countries with fewer than X cases
                                      vega-values
                                      (apply conj vega-values
                                             (map (fn [[d c]]
                                                    {:date d
                                                     :cases c
                                                     :country country})
                                                  date->cases))))
                                  [])
                          ;; purely for our human reading convenience:
                          (sort-by (juxt :country :date)))}
              :mark {:type "line" :strokeWidth 4 :point "transparent"}
              :encoding {:x {:field "date", :type "temporal"},
                         :y {:field "cases", :type "quantitative"}
                         ;; TODO tooltip for country
                         :color {:field "country", :type "nominal"}
                         :tooltip {:field "country", :type "nominal"}}}))


;;;; ===========================================================================
;;;; Choropleth: European countries' COVID19 rate of infection
;; geojson from https://github.com/leakyMirror/map-of-europe/blob/master/GeoJSON/europe.geojson

(def europe-dimensions
  {:width 750 :height 750})

(def europe-geojson
  (json/read-value (java.io.File. "resources/public/public/data/europe.geo.json")
                   (json/object-mapper {:decode-key-fn true})))

(comment
  ;; Which countries are in this geoJSON?
  (map :NAME (map :properties (:features europe-geojson)))
  ;; ("Azerbaijan" "Albania" "Armenia" "Bosnia and Herzegovina" "Bulgaria" "Cyprus" "Denmark" "Ireland" "Estonia" "Austria" "Czech Republic" "Finland" "France" "Georgia" "Germany" "Greece" "Croatia" "Hungary" "Iceland" "Israel" "Italy" "Latvia" "Belarus" "Lithuania" "Slovakia" "Liechtenstein" "The former Yugoslav Republic of Macedonia" "Malta" "Belgium" "Faroe Islands" "Andorra" "Luxembourg" "Monaco" "Montenegro" "Netherlands" "Norway" "Poland" "Portugal" "Romania" "Republic of Moldova" "Slovenia" "Spain" "Sweden" "Switzerland" "Turkey" "United Kingdom" "Ukraine" "San Marino" "Serbia" "Holy See (Vatican City)" "Russia")

  (clojure.set/difference (set (map (comp #(get world-bank/normalize-country % %) :NAME)
                                    (map :properties (:features europe-geojson))))
                          jh/countries)

  ;; What outliers are making the map less useful?
  (sort-by second (map (juxt (comp :NAME :properties) :rate)
                       (:features
                        (update europe-geojson
                                :features
                                (fn [features]
                                  (mapv (fn [feature]
                                          (let [cntry (:NAME (:properties feature))]
                                            (assoc feature
                                                   :country cntry
                                                   :rate (jh/rate-as-of :confirmed cntry 1))))
                                        features))))))

  (jh/new-daily-cases-in :deaths "Andorra")

  )

(def europe-infection-datapoints
  (update europe-geojson :features
          (fn [features]
            (mapv (fn [feature]
                    (let [cntry (:NAME (:properties feature))]
                      (assoc feature
                             :country cntry
                             ;; Because some countries (e.g. Germany) are
                             ;; not testing people post-mortem, which
                             ;; drastically reduces their reported
                             ;; deaths, I chose to calculate only
                             ;; confirmed cases.
                             :confirmed-rate (jh/rate-as-of :confirmed cntry 1))))
                  features))))

(comment
  ;; Let's look at the rate of change data
  (sort-by second (map (juxt :country :confirmed-rate) (:features europe-infection-datapoints)))

  )

(oz/view!
 (merge-with merge oz-config europe-dimensions
             {:title {:text "COVID19 in Europe: Rate of Infection Increase"}
              :data {:values europe-infection-datapoints
                     :format {:type "json" :property "features"}}
              :mark {:type "geoshape" :stroke "white" :strokeWidth 1}
              :encoding {:color {:field "confirmed-rate" :type "quantitative"
                                 :scale {:domain [0 (->> (:features europe-infection-datapoints)
                                                         ;; Nix the outlier TODO automate this
                                                         (remove (comp #{"Andorra"} :country))
                                                         (map :confirmed-rate)
                                                         (apply max))]
                                         :range [;;"#f6f6f6"
                                                 (:blue applied-science-palette)
                                                 (:green applied-science-palette)]}}
                         :tooltip [{:field "country" :type "nominal"}
                                   {:field "confirmed-rate" :type "quantitative"}]}
              :selection {:highlight {:on "mouseover" :type "single"}}}))
