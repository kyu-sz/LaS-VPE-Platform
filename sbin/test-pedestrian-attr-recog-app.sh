#!/usr/bin/env bash
java -Djava.library.path=$LD_LIBRARY_PATH:lib/linux \
    -classpath bin/las-vpe-platform-0.0.1-full.jar:bin/las-vpe-platform-0.0.1-tests.jar:lib/linux \
    org.cripac.isee.vpe.alg.PedestrianAttrRecogAppTest \
    --app-property-file conf/pedestrian-attr-recog/app.properties
