# Java
    export JAVA_HOME=/usr/local/Cellar/openjdk/19.0.2 
    export PATH="$JAVA_HOME/bin/:$PATH"
# Spark
```
    export SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.2/libexec
    export PATH="$SPARK_HOME/bin/:$PATH"
```

# PySpark
```
    export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
    export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"
```

export JAVA_HOME=/usr/local/Cellar/openjdk/19.0.2 
export PATH="$JAVA_HOME/bin/:$PATH"
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.2/libexec
export PATH="$SPARK_HOME/bin/:$PATH"
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"