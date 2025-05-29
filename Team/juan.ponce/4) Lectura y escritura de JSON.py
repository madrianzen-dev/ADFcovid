# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ## Lectura y escritura en Parquet y Delta Lake
# MAGIC
# MAGIC JSON (Notación de Objetos JavaScript) es un formato ligero basado en texto para estructurar datos.
# MAGIC
# MAGIC JSON representa los datos como pares clave-valor mediante objetos ({}) y matrices ([]).
# MAGIC
# MAGIC Admite tipos de datos básicos como cadenas, números, booleanos, valores nulos y estructuras anidadas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Links and Resources
# MAGIC - [Spark SQL API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)
# MAGIC - [JSON Introduction](https://www.w3schools.com/js/js_json_intro.asp)
# MAGIC - [dbutils Documentation](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils#dbutils-fs-rm)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# Define the schema explicitly using StructType and StructField
schema = StructType([
    StructField("country_name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("population", LongType(), True),
    StructField("area_km2", IntegerType(), True)
])

# Data for 195 countries (each tuple: country_name, continent, population, area_km2)
data = [
    ("Afghanistan", "Asia", 38928346, 652230),
    ("Albania", "Europe", 2877797, 28748),
    ("Algeria", "Africa", 43851044, 2381741),
    ("Andorra", "Europe", 77265, 468),
    ("Angola", "Africa", 32866272, 1246700),
    ("Antigua and Barbuda", "North America", 97929, 442),
    ("Argentina", "South America", 45376763, 2780400),
    ("Armenia", "Asia", 2963243, 29743),
    ("Australia", "Oceania", 25649984, 7692024),
    ("Austria", "Europe", 9006398, 83879),
    ("Azerbaijan", "Asia", 10139177, 86600),
    ("Bahamas", "North America", 393244, 13943),
    ("Bahrain", "Asia", 1701575, 765),
    ("Bangladesh", "Asia", 164689383, 147570),
    ("Barbados", "North America", 287375, 430),
    ("Belarus", "Europe", 9449323, 207600),
    ("Belgium", "Europe", 11589623, 30528),
    ("Belize", "North America", 397621, 22966),
    ("Benin", "Africa", 12123200, 114763),
    ("Bhutan", "Asia", 771608, 38394),
    ("Bolivia", "South America", 11673021, 1098581),
    ("Bosnia and Herzegovina", "Europe", 3280819, 51129),
    ("Botswana", "Africa", 2351627, 581730),
    ("Brazil", "South America", 212559417, 8515767),
    ("Brunei", "Asia", 437479, 5765),
    ("Bulgaria", "Europe", 6948445, 110994),
    ("Burkina Faso", "Africa", 20903273, 274200),
    ("Burundi", "Africa", 11890784, 27834),
    ("Cabo Verde", "Africa", 555987, 4033),
    ("Cambodia", "Asia", 16718965, 181035),
    ("Cameroon", "Africa", 26545863, 475442),
    ("Canada", "North America", 37742154, 9984670),
    ("Central African Republic", "Africa", 4829767, 622984),
    ("Chad", "Africa", 16425864, 1284000),
    ("Chile", "South America", 19116201, 756102),
    ("China", "Asia", 1439323776, 9596961),
    ("Colombia", "South America", 50882891, 1141748),
    ("Comoros", "Africa", 869601, 1862),
    ("Congo, Democratic Republic of the", "Africa", 89561403, 2344858),
    ("Congo, Republic of the", "Africa", 5518087, 342000),
    ("Costa Rica", "North America", 5094118, 51100),
    ("Croatia", "Europe", 4105267, 56594),
    ("Cuba", "North America", 11326616, 109884),
    ("Côte d'Ivoire", "Africa", 26378274, 322463),
    ("Cyprus", "Europe", 1207359, 9251),
    ("Czech Republic", "Europe", 10708981, 78866),
    ("Denmark", "Europe", 5792202, 42933),
    ("Djibouti", "Africa", 988000, 23200),
    ("Dominica", "North America", 71986, 751),
    ("Dominican Republic", "North America", 10847910, 48671),
    ("Ecuador", "South America", 17643054, 283561),
    ("Egypt", "Africa", 102334404, 1002450),
    ("El Salvador", "North America", 6486205, 21041),
    ("Equatorial Guinea", "Africa", 1402985, 28051),
    ("Eritrea", "Africa", 3546421, 117600),
    ("Estonia", "Europe", 1331796, 45227),
    ("Eswatini", "Africa", 1160164, 17364),
    ("Ethiopia", "Africa", 114963588, 1104300),
    ("Fiji", "Oceania", 896444, 18274),
    ("Finland", "Europe", 5540720, 338424),
    ("France", "Europe", 65273511, 551695),
    ("Gabon", "Africa", 2225728, 267668),
    ("Gambia", "Africa", 2416668, 11295),
    ("Georgia", "Asia", 3989167, 69700),
    ("Germany", "Europe", 83783942, 357022),
    ("Ghana", "Africa", 31072940, 238533),
    ("Greece", "Europe", 10423054, 131957),
    ("Grenada", "North America", 112523, 344),
    ("Guatemala", "North America", 17915568, 108889),
    ("Guinea", "Africa", 13132795, 245857),
    ("Guinea-Bissau", "Africa", 1968001, 36125),
    ("Guyana", "South America", 786552, 214969),
    ("Haiti", "North America", 11402528, 27750),
    ("Holy See", "Europe", 801, 0),
    ("Honduras", "North America", 9904607, 112492),
    ("Hungary", "Europe", 9660351, 93030),
    ("Iceland", "Europe", 341243, 103000),
    ("India", "Asia", 1380004385, 3287263),
    ("Indonesia", "Asia", 273523615, 1904569),
    ("Iran", "Asia", 83992949, 1648195),
    ("Iraq", "Asia", 40222493, 438317),
    ("Ireland", "Europe", 4937786, 70273),
    ("Israel", "Asia", 9053300, 20770),
    ("Italy", "Europe", 60461826, 301340),
    ("Jamaica", "North America", 2961167, 10991),
    ("Japan", "Asia", 126476461, 377975),
    ("Jordan", "Asia", 10203134, 89342),
    ("Kazakhstan", "Asia", 18776707, 2724900),
    ("Kenya", "Africa", 53771296, 580367),
    ("Kiribati", "Oceania", 119449, 811),
    ("Kuwait", "Asia", 4270571, 17818),
    ("Kyrgyzstan", "Asia", 6524195, 199951),
    ("Laos", "Asia", 7275560, 236800),
    ("Latvia", "Europe", 1886198, 64559),
    ("Lebanon", "Asia", 6825445, 10452),
    ("Lesotho", "Africa", 2142249, 30355),
    ("Liberia", "Africa", 5073296, 111369),
    ("Libya", "Africa", 6871292, 1759540),
    ("Liechtenstein", "Europe", 38128, 160),
    ("Lithuania", "Europe", 2722289, 65300),
    ("Luxembourg", "Europe", 634814, 2586),
    ("Madagascar", "Africa", 27691018, 587041),
    ("Malawi", "Africa", 19129952, 118484),
    ("Malaysia", "Asia", 32365999, 330803),
    ("Maldives", "Asia", 540544, 300),
    ("Mali", "Africa", 20250833, 1240192),
    ("Malta", "Europe", 441543, 316),
    ("Marshall Islands", "Oceania", 59190, 181),
    ("Mauritania", "Africa", 4649658, 1030700),
    ("Mauritius", "Africa", 1271768, 2040),
    ("Mexico", "North America", 128932753, 1964375),
    ("Micronesia", "Oceania", 115023, 702),
    ("Moldova", "Europe", 2640438, 33851),
    ("Monaco", "Europe", 39242, 2),
    ("Mongolia", "Asia", 3278290, 1564110),
    ("Montenegro", "Europe", 622359, 13812),
    ("Morocco", "Africa", 36910560, 446550),
    ("Mozambique", "Africa", 31255435, 801590),
    ("Myanmar", "Asia", 54409800, 676578),
    ("Namibia", "Africa", 2540905, 824292),
    ("Nauru", "Oceania", 10824, 21),
    ("Nepal", "Asia", 29136808, 147181),
    ("Netherlands", "Europe", 17134872, 41543),
    ("New Zealand", "Oceania", 5084300, 268838),
    ("Nicaragua", "North America", 6624554, 130373),
    ("Niger", "Africa", 24206644, 1267000),
    ("Nigeria", "Africa", 206139589, 923768),
    ("North Korea", "Asia", 25778816, 120538),
    ("North Macedonia", "Europe", 2083374, 25713),
    ("Norway", "Europe", 5421241, 385207),
    ("Oman", "Asia", 5106626, 309500),
    ("Pakistan", "Asia", 220892340, 881913),
    ("Palau", "Oceania", 18094, 459),
    ("Palestine, State of", "Asia", 5101414, 6020),
    ("Panama", "North America", 4314767, 75417),
    ("Papua New Guinea", "Oceania", 8947024, 462840),
    ("Paraguay", "South America", 7132538, 406752),
    ("Peru", "South America", 32971854, 1285216),
    ("Philippines", "Asia", 109581078, 300000),
    ("Poland", "Europe", 37846611, 312696),
    ("Portugal", "Europe", 10196709, 92090),
    ("Qatar", "Asia", 2881053, 11586),
    ("Romania", "Europe", 19237691, 238391),
    ("Russia", "Europe", 144478050, 17098242),
    ("Rwanda", "Africa", 12952218, 26338),
    ("Saint Kitts and Nevis", "North America", 53192, 261),
    ("Saint Lucia", "North America", 183627, 616),
    ("Saint Vincent and the Grenadines", "North America", 110940, 389),
    ("Samoa", "Oceania", 198414, 2842),
    ("San Marino", "Europe", 33938, 61),
    ("Sao Tome and Principe", "Africa", 219159, 964),
    ("Saudi Arabia", "Asia", 34813871, 2149690),
    ("Senegal", "Africa", 16743927, 196722),
    ("Serbia", "Europe", 6944975, 88361),
    ("Seychelles", "Africa", 98347, 455),
    ("Sierra Leone", "Africa", 7976983, 71740),
    ("Singapore", "Asia", 5850342, 719),
    ("Slovakia", "Europe", 5459642, 49037),
    ("Slovenia", "Europe", 2078938, 20273),
    ("Solomon Islands", "Oceania", 686884, 28896),
    ("Somalia", "Africa", 15893222, 637657),
    ("South Africa", "Africa", 59308690, 1221037),
    ("South Korea", "Asia", 51269185, 100210),
    ("South Sudan", "Africa", 11193725, 644329),
    ("Spain", "Europe", 46754778, 505992),
    ("Sri Lanka", "Asia", 21413249, 65610),
    ("Sudan", "Africa", 43849260, 1861484),
    ("Suriname", "South America", 586634, 163820),
    ("Sweden", "Europe", 10099265, 450295),
    ("Switzerland", "Europe", 8654622, 41285),
    ("Syria", "Asia", 17500657, 185180),
    ("Tajikistan", "Asia", 9537645, 143100),
    ("Tanzania", "Africa", 59734218, 945087),
    ("Thailand", "Asia", 69799978, 513120),
    ("Timor-Leste", "Asia", 1318445, 14874),
    ("Togo", "Africa", 8278724, 56785),
    ("Tonga", "Oceania", 105695, 747),
    ("Trinidad and Tobago", "North America", 1399488, 5130),
    ("Tunisia", "Africa", 11818619, 163610),
    ("Turkey", "Asia", 84339067, 783562),
    ("Turkmenistan", "Asia", 6031200, 488100),
    ("Tuvalu", "Oceania", 11792, 26),
    ("Uganda", "Africa", 45741007, 241550),
    ("Ukraine", "Europe", 43733762, 603628),
    ("United Arab Emirates", "Asia", 9890400, 83600),
    ("United Kingdom", "Europe", 67886011, 243610),
    ("United States", "North America", 331002651, 9833520),
    ("Uruguay", "South America", 3473730, 176215),
    ("Uzbekistan", "Asia", 33469203, 447400),
    ("Vanuatu", "Oceania", 307145, 12189),
    ("Venezuela", "South America", 28435940, 916445),
    ("Vietnam", "Asia", 97338579, 331212),
    ("Yemen", "Asia", 29825964, 527968),
    ("Zambia", "Africa", 18383955, 752612),
    ("Zimbabwe", "Africa", 14862924, 390757)
]

# Create the DataFrame using the defined schema
df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

# Displaying the DataFrame
display(df)

# COMMAND ----------

# Writing DataFrame to JSON format

df.write.json("abfss://data@dbstoragezat11.dfs.core.windows.net/json/countries_json",mode="overwrite")
 

# COMMAND ----------

# Displaying the directory contents
display(dbutils.fs.ls("abfss://inputs@dbstoragezat10.dfs.core.windows.net/write_demo/countries_json"))

# COMMAND ----------

# Previewing the raw contents of a partitioned JSON file
print(dbutils.fs.head("abfss://inputs@dbstoragezat10.dfs.core.windows.net/write_demo/countries_json/part-00000-tid-87624903030054783-83af69db-1182-4c32-9091-b438fc2f52fe-0-1-c000.json"))

# COMMAND ----------

# Las 3 líneas siguientes son equivalentes

df.write.json("abfss://inputs@dbstoragezat10.dfs.core.windows.net/write_demo/countries_json", mode="overwrite")
df.write.mode("overwrite").json("abfss://inputs@dbstoragezat10.dfs.core.windows.net/write_demo/countries_json")
df.write.format("json").mode("overwrite").save("abfss://inputs@dbstoragezat10.dfs.core.windows.net/write_demo/countries_json")

# COMMAND ----------

# Lectura y visualización de archivos JSON
# Ambas líneas son equivalentes
spark.read.json("abfss://inputs@dbstoragezat10.dfs.core.windows.net/write_demo/countries_json2").display()
#spark.read.format("json").option("multiLine", False).load("/FileStore/write_demo/countries_json").display()