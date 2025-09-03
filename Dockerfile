# בסיס
FROM python:3.11-slim

# התקנת Java
RUN apt-get update && \
   apt-get install -y default-jre wget curl && \
   apt-get clean
# הגדרת משתני סביבה ל-Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH
# התקנת PySpark
RUN pip install pyspark==3.5.1

# תיקייה לעבודה
WORKDIR /app
# העתקת קבצים
COPY sales.csv /app/
COPY sales_analysis.py /app/
# הפקודה להרצה
CMD ["python", "sales_analysis.py"]