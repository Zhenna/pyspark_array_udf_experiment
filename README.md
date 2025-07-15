# pyspark_array_udf_experiment

### Pre-requisites

Please make sure you have Java installed. PySpark needs Java to launch the JVM backend.

➤ Check Java:
```bash
java -version
```

If it doesn't print a Java version, install Java 11 or 17 and set `JAVA_HOME`.

➤ Set `JAVA_HOME` in Python:
Add this before importing `SparkSession`:

```python
import os
os.environ["JAVA_HOME"] = "/your/java/path"  # ← Replace with your actual Java path
```

Typical paths:

- macOS with Homebrew: `/opt/homebrew/opt/openjdk@17`

- Linux: `/usr/lib/jvm/java-17-openjdk-amd64`

- Windows: `C:\\Program Files\\Java\\jdk-17\\`

## Install dependencies

```bash
pip install -r requirements.txt
```

