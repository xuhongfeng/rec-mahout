#/bin/bash
mvn package -Dmaven.test.skip=true
scp target/rec-mahout-0.1-jar-with-dependencies.jar chenatu@hd1:~/tmp/temp.jar
