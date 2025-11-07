FROM docker.io/library/spark:3.5.2 

RUN curl -L -O http://search.maven.org/remotecontent?filepath=org/apache/ivy/ivy/2.5.3/ivy-2.5.3.jar

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -notransitive \
-dependency org.apache.hadoop hadoop-client-api 3.3.4 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -confs compile \
-dependency org.apache.hadoop hadoop-client-api 3.3.4 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -notransitive \
-dependency org.apache.hadoop hadoop-client-runtime 3.3.4 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -confs compile \
-dependency org.apache.hadoop hadoop-client-runtime 3.3.4 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -notransitive \
-dependency org.apache.hadoop hadoop-aws 3.3.4 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -confs compile \
-dependency org.apache.hadoop hadoop-aws 3.3.4 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -notransitive \
-dependency org.apache.hadoop hadoop-common 3.3.4 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -confs compile \
-dependency org.apache.hadoop hadoop-common 3.3.4 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -notransitive \
-dependency com.amazonaws aws-java-sdk-bundle 1.12.162 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"

RUN java -Divy.cache.dir=/tmp -Divy.home=/tmp -jar ivy-2.5.3.jar -confs compile \
-dependency com.amazonaws aws-java-sdk-bundle 1.12.162 \
-retrieve "/opt/spark/jars/[artifact]-[revision](-[classifier]).[ext]"