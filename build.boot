(def project 'fmine)
(def version "0.1.0-SNAPSHOT")

(set-env! :resource-paths #{"src"}
          :source-paths   #{"test"}
          :dependencies   '[[org.clojure/clojure "RELEASE"]
                            [adzerk/boot-test "RELEASE" :scope "test"]
                            [hickory "0.7.1"]
                            [proto-repl "0.3.1"]
                            [org.clojure/core.async "0.4.474"]
                            [clj-http "3.7.0"]])

(task-options!
 aot {:namespace   #{'fmine.core}}
 pom {:project     project
      :version     version
      :description "FIXME: write description"
      :url         "http://example/FIXME"
      :scm         {:url "https://github.com/fuentesjr/fmine"}
      :license     {"Eclipse Public License"
                    "http://www.eclipse.org/legal/epl-v10.html"}}
 jar {:main        'fmine.core
      :file        (str "fmine-" version "-standalone.jar")})

(deftask build
  "Build the project locally as a JAR."
  [d dir PATH #{str} "the set of directories to write to (target)."]
  (let [dir (if (seq dir) dir #{"target"})]
    (comp (aot) (pom) (uber) (jar) (target :dir dir))))

(deftask run
  "Run the project."
  [a args ARG [str] "the arguments for the application."]
  (require '[fmine.core :as app])
  (apply (resolve 'app/-main) args))

(require '[adzerk.boot-test :refer [test]])
