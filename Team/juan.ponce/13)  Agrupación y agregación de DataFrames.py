# Databricks notebook source
# MAGIC %md
# MAGIC ## Agrupación y agregación de DataFrames
# MAGIC La agrupación y agregación en PySpark permite resumir datos mediante la aplicación de funciones de agregación (por ejemplo, suma, promedio, recuento) a subconjuntos agrupados de un DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Links and Resources
# MAGIC - [groupBy()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)
# MAGIC - [Aggregate Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#aggregate-functions)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# Refined schema to include 'sub_region'
schema = StructType([
    StructField("country_name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("sub_region", StringType(), True),
    StructField("population", LongType(), True),
    StructField("area_km2", IntegerType(), True)
])

# Refactor your data to include 'sub_region' for each country
data = [
    ("Afghanistan", "Asia", "Southern Asia", 38928346, 652230),
    ("Albania", "Europe", "Southern Europe", 2877797, 28748),
    ("Algeria", "Africa", "Northern Africa", 43851044, 2381741),
    ("Andorra", "Europe", "Southern Europe", 77265, 468),
    ("Angola", "Africa", "Middle Africa", 32866272, 1246700),
    ("Antigua and Barbuda", "North America", "Caribbean", 97929, 442),
    ("Argentina", "South America", "South America", 45376763, 2780400),
    ("Armenia", "Asia", "Western Asia", 2963243, 29743),
    ("Australia", "Oceania", "Australia and New Zealand", 25649984, 7692024),
    ("Austria", "Europe", "Western Europe", 9006398, 83879),
    ("Azerbaijan", "Asia", "Western Asia", 10139177, 86600),
    ("Bahamas", "North America", "Caribbean", 393244, 13943),
    ("Bahrain", "Asia", "Western Asia", 1701575, 765),
    ("Bangladesh", "Asia", "Southern Asia", 164689383, 147570),
    ("Barbados", "North America", "Caribbean", 287375, 430),
    ("Belarus", "Europe", "Eastern Europe", 9449323, 207600),
    ("Belgium", "Europe", "Western Europe", 11589623, 30528),
    ("Belize", "North America", "Central America", 397621, 22966),
    ("Benin", "Africa", "Western Africa", 12123200, 114763),
    ("Bhutan", "Asia", "Southern Asia", 771608, 38394),
    ("Bolivia", "South America", "South America", 11673021, 1098581),
    ("Bosnia and Herzegovina", "Europe", "Southern Europe", 3280819, 51129),
    ("Botswana", "Africa", "Southern Africa", 2351627, 581730),
    ("Brazil", "South America", "South America", 212559417, 8515767),
    ("Brunei", "Asia", "South-Eastern Asia", 437479, 5765),
    ("Bulgaria", "Europe", "Eastern Europe", 6948445, 110994),
    ("Burkina Faso", "Africa", "Western Africa", 20903273, 274200),
    ("Burundi", "Africa", "Eastern Africa", 11890784, 27834),
    ("Cabo Verde", "Africa", "Western Africa", 555987, 4033),
    ("Cambodia", "Asia", "South-Eastern Asia", 16718965, 181035),
    ("Cameroon", "Africa", "Middle Africa", 26545863, 475442),
    ("Canada", "North America", "Northern America", 37742154, 9984670),
    ("Central African Republic", "Africa", "Middle Africa", 4829767, 622984),
    ("Chad", "Africa", "Middle Africa", 16425864, 1284000),
    ("Chile", "South America", "South America", 19116201, 756102),
    ("China", "Asia", "Eastern Asia", 1439323776, 9596961),
    ("Colombia", "South America", "South America", 50882891, 1141748),
    ("Comoros", "Africa", "Eastern Africa", 869601, 1862),
    ("Congo, Democratic Republic of the", "Africa", "Middle Africa", 89561403, 2344858),
    ("Congo, Republic of the", "Africa", "Middle Africa", 5518087, 342000),
    ("Costa Rica", "North America", "Central America", 5094118, 51100),
    ("Croatia", "Europe", "Southern Europe", 4105267, 56594),
    ("Cuba", "North America", "Caribbean", 11326616, 109884),
    ("Côte d'Ivoire", "Africa", "Western Africa", 26378274, 322463),
    ("Cyprus", "Europe", "Western Asia", 1207359, 9251),
    ("Czech Republic", "Europe", "Eastern Europe", 10708981, 78866),
    ("Denmark", "Europe", "Northern Europe", 5792202, 42933),
    ("Djibouti", "Africa", "Eastern Africa", 988000, 23200),
    ("Dominica", "North America", "Caribbean", 71986, 751),
    ("Dominican Republic", "North America", "Caribbean", 10847910, 48671),
    ("Ecuador", "South America", "South America", 17643054, 283561),
    ("Egypt", "Africa", "Northern Africa", 102334404, 1002450),
    ("El Salvador", "North America", "Central America", 6486205, 21041),
    ("Equatorial Guinea", "Africa", "Middle Africa", 1402985, 28051),
    ("Eritrea", "Africa", "Eastern Africa", 3546421, 117600),
    ("Estonia", "Europe", "Northern Europe", 1331796, 45227),
    ("Eswatini", "Africa", "Southern Africa", 1160164, 17364),
    ("Ethiopia", "Africa", "Eastern Africa", 114963588, 1104300),
    ("Fiji", "Oceania", "Melanesia", 896444, 18274),
    ("Finland", "Europe", "Northern Europe", 5540720, 338424),
    ("France", "Europe", "Western Europe", 65273511, 551695),
    ("Gabon", "Africa", "Middle Africa", 2225728, 267668),
    ("Gambia", "Africa", "Western Africa", 2416668, 11295),
    ("Georgia", "Asia", "Western Asia", 3989167, 69700),
    ("Germany", "Europe", "Western Europe", 83783942, 357022),
    ("Ghana", "Africa", "Western Africa", 31072940, 238533),
    ("Greece", "Europe", "Southern Europe", 10423054, 131957),
    ("Grenada", "North America", "Caribbean", 112523, 344),
    ("Guatemala", "North America", "Central America", 17915568, 108889),
    ("Guinea", "Africa", "Western Africa", 13132795, 245857),
    ("Guinea-Bissau", "Africa", "Western Africa", 1968001, 36125),
    ("Guyana", "South America", "South America", 786552, 214969),
    ("Haiti", "North America", "Caribbean", 11402528, 27750),
    ("Holy See", "Europe", "Southern Europe", 801, 0),
    ("Honduras", "North America", "Central America", 9904607, 112492),
    ("Hungary", "Europe", "Eastern Europe", 9660351, 93030),
    ("Iceland", "Europe", "Northern Europe", 341243, 103000),
    ("India", "Asia", "Southern Asia", 1380004385, 3287263),
    ("Indonesia", "Asia", "South-Eastern Asia", 273523615, 1904569),
    ("Iran", "Asia", "Southern Asia", 83992949, 1648195),
    ("Iraq", "Asia", "Western Asia", 40222493, 438317),
    ("Ireland", "Europe", "Northern Europe", 4937786, 70273),
    ("Israel", "Asia", "Western Asia", 9053300, 20770),
    ("Italy", "Europe", "Southern Europe", 60461826, 301340),
    ("Jamaica", "North America", "Caribbean", 2961167, 10991),
    ("Japan", "Asia", "Eastern Asia", 126476461, 377975),
    ("Jordan", "Asia", "Western Asia", 10203134, 89342),
    ("Kazakhstan", "Asia", "Central Asia", 18776707, 2724900),
    ("Kenya", "Africa", "Eastern Africa", 53771296, 580367),
    ("Kiribati", "Oceania", "Micronesia", 119449, 811),
    ("Kuwait", "Asia", "Western Asia", 4270571, 17818),
    ("Kyrgyzstan", "Asia", "Central Asia", 6524195, 199951),
    ("Laos", "Asia", "South-Eastern Asia", 7275560, 236800),
    ("Latvia", "Europe", "Northern Europe", 1886198, 64559),
    ("Lebanon", "Asia", "Western Asia", 6825445, 10452),
    ("Lesotho", "Africa", "Southern Africa", 2142249, 30355),
    ("Liberia", "Africa", "Western Africa", 5073296, 111369),
    ("Libya", "Africa", "Northern Africa", 6871292, 1759540),
    ("Liechtenstein", "Europe", "Western Europe", 38128, 160),
    ("Lithuania", "Europe", "Northern Europe", 2722289, 65300),
    ("Luxembourg", "Europe", "Western Europe", 634814, 2586),
    ("Madagascar", "Africa", "Eastern Africa", 27691018, 587041),
    ("Malawi", "Africa", "Eastern Africa", 19129952, 118484),
    ("Malaysia", "Asia", "South-Eastern Asia", 32365999, 330803),
    ("Maldives", "Asia", "Southern Asia", 540544, 300),
    ("Mali", "Africa", "Western Africa", 20250833, 1240192),
    ("Malta", "Europe", "Southern Europe", 441543, 316),
    ("Marshall Islands", "Oceania", "Micronesia", 59190, 181),
    ("Mauritania", "Africa", "Western Africa", 4649658, 1030700),
    ("Mauritius", "Africa", "Eastern Africa", 1271768, 2040),
    ("Mexico", "North America", "Central America", 128932753, 1964375),
    ("Micronesia", "Oceania", "Micronesia", 115023, 702),
    ("Moldova", "Europe", "Eastern Europe", 2640438, 33851),
    ("Monaco", "Europe", "Western Europe", 39242, 2),
    ("Mongolia", "Asia", "Eastern Asia", 3278290, 1564110),
    ("Montenegro", "Europe", "Southern Europe", 622359, 13812),
    ("Morocco", "Africa", "Northern Africa", 36910560, 446550),
    ("Mozambique", "Africa", "Eastern Africa", 31255435, 801590),
    ("Myanmar", "Asia", "South-Eastern Asia", 54409800, 676578),
    ("Namibia", "Africa", "Southern Africa", 2540905, 824292),
    ("Nauru", "Oceania", "Micronesia", 10824, 21),
    ("Nepal", "Asia", "Southern Asia", 29136808, 147181),
    ("Netherlands", "Europe", "Western Europe", 17134872, 41543),
    ("New Zealand", "Oceania", "Australia and New Zealand", 5084300, 268838),
    ("Nicaragua", "North America", "Central America", 6624554, 130373),
    ("Niger", "Africa", "Western Africa", 24206644, 1267000),
    ("Nigeria", "Africa", "Western Africa", 206139589, 923768),
    ("North Korea", "Asia", "Eastern Asia", 25778816, 120538),
    ("North Macedonia", "Europe", "Southern Europe", 2083374, 25713),
    ("Norway", "Europe", "Northern Europe", 5421241, 385207),
    ("Oman", "Asia", "Western Asia", 5106626, 309500),
    ("Pakistan", "Asia", "Southern Asia", 220892340, 881913),
    ("Palau", "Oceania", "Micronesia", 18094, 459),
    ("Palestine, State of", "Asia", "Western Asia", 5101414, 6020),
    ("Panama", "North America", "Central America", 4314767, 75417),
    ("Papua New Guinea", "Oceania", "Melanesia", 8947024, 462840),
    ("Paraguay", "South America", "South America", 7132538, 406752),
    ("Peru", "South America", "South America", 32971854, 1285216),
    ("Philippines", "Asia", "South-Eastern Asia", 109581078, 300000),
    ("Poland", "Europe", "Eastern Europe", 37846611, 312696),
    ("Portugal", "Europe", "Southern Europe", 10196709, 92090),
    ("Qatar", "Asia", "Western Asia", 2881053, 11586),
    ("Romania", "Europe", "Eastern Europe", 19237691, 238391),
    ("Russia", "Europe", "Eastern Europe", 144478050, 17098242),
    ("Rwanda", "Africa", "Eastern Africa", 12952218, 26338),
    ("Saint Kitts and Nevis", "North America", "Caribbean", 53192, 261),
    ("Saint Lucia", "North America", "Caribbean", 183627, 616),
    ("Saint Vincent and the Grenadines", "North America", "Caribbean", 110940, 389),
    ("Samoa", "Oceania", "Polynesia", 198414, 2842),
    ("San Marino", "Europe", "Southern Europe", 33938, 61),
    ("Sao Tome and Principe", "Africa", "Middle Africa", 219159, 964),
    ("Saudi Arabia", "Asia", "Western Asia", 34813871, 2149690),
    ("Senegal", "Africa", "Western Africa", 16743927, 196722),
    ("Serbia", "Europe", "Southern Europe", 6944975, 88361),
    ("Seychelles", "Africa", "Eastern Africa", 98347, 455),
    ("Sierra Leone", "Africa", "Western Africa", 7976983, 71740),
    ("Singapore", "Asia", "South-Eastern Asia", 5850342, 719),
    ("Slovakia", "Europe", "Eastern Europe", 5459642, 49037),
    ("Slovenia", "Europe", "Southern Europe", 2078938, 20273),
    ("Solomon Islands", "Oceania", "Melanesia", 686884, 28896),
    ("Somalia", "Africa", "Eastern Africa", 15893222, 637657),
    ("South Africa", "Africa", "Southern Africa", 59308690, 1221037),
    ("South Korea", "Asia", "Eastern Asia", 51269185, 100210),
    ("South Sudan", "Africa", "Eastern Africa", 11193725, 644329),
    ("Spain", "Europe", "Southern Europe", 46754778, 505992),
    ("Sri Lanka", "Asia", "Southern Asia", 21413249, 65610),
    ("Sudan", "Africa", "Northern Africa", 43849260, 1861484),
    ("Suriname", "South America", "South America", 586634, 163820),
    ("Sweden", "Europe", "Northern Europe", 10099265, 450295),
    ("Switzerland", "Europe", "Western Europe", 8654622, 41285),
    ("Syria", "Asia", "Western Asia", 17500657, 185180),
    ("Tajikistan", "Asia", "Central Asia", 9537645, 143100),
    ("Tanzania", "Africa", "Eastern Africa", 59734218, 945087),
    ("Thailand", "Asia", "South-Eastern Asia", 69799978, 513120),
    ("Timor-Leste", "Asia", "South-Eastern Asia", 1318445, 14874),
    ("Togo", "Africa", "Western Africa", 8278724, 56785),
    ("Tonga", "Oceania", "Polynesia", 105695, 747),
    ("Trinidad and Tobago", "North America", "Caribbean", 1399488, 5130),
    ("Tunisia", "Africa", "Northern Africa", 11818619, 163610),
    ("Turkey", "Asia", "Western Asia", 84339067, 783562),
    ("Turkmenistan", "Asia", "Central Asia", 6031200, 488100),
    ("Tuvalu", "Oceania", "Polynesia", 11792, 26),
    ("Uganda", "Africa", "Eastern Africa", 45741007, 241550),
    ("Ukraine", "Europe", "Eastern Europe", 43733762, 603628),
    ("United Arab Emirates", "Asia", "Western Asia", 9890400, 83600),
    ("United Kingdom", "Europe", "Northern Europe", 67886011, 243610),
    ("United States", "North America", "Northern America", 331002651, 9833520),
    ("Uruguay", "South America", "South America", 3473730, 176215),
    ("Uzbekistan", "Asia", "Central Asia", 33469203, 447400),
    ("Vanuatu", "Oceania", "Melanesia", 307145, 12189),
    ("Venezuela", "South America", "South America", 28435940, 916445),
    ("Vietnam", "Asia", "South-Eastern Asia", 97338579, 331212),
    ("Yemen", "Asia", "Western Asia", 29825964, 527968),
    ("Zambia", "Africa", "Eastern Africa", 18383955, 752612),
    ("Zimbabwe", "Africa", "Eastern Africa", 14862924, 390757)
]

# Create the DataFrame using the new schema (with sub_region)
df = spark.createDataFrame(data, schema)

# Display the DataFrame
df.show(5, truncate=False)

# COMMAND ----------

# Devuelve un objeto GroupedData

type(df.groupBy("continent"))

# COMMAND ----------

from pyspark.sql.functions import sum, col, avg

# COMMAND ----------

# Agrupa el DataFrame por continente, calcula la suma de la población

df.groupBy("continent").sum("population").display()

# COMMAND ----------

# sum() devuelve un DataFrame, por lo que no puede encadenar otro agregador como avg() en él después

df.groupBy("continent").agg({'population': 'sum', 'area_km2': 'avg'}).display()

# COMMAND ----------

# El uso de .agg() nos permite realizar múltiples funciones de agregación en un solo paso

df.groupBy("continent").agg(
                                sum("population"),
                                avg("population")
                            ).display()

# COMMAND ----------

# El uso de .alias() asigna nombres significativos a las columnas agregadas

df.groupBy("continent").agg(
                                sum("population").alias("total_population"),
                                avg("population").alias("avg_population"),
                                sum("area_km2").alias("total_area_km2")
                            ).display()

# COMMAND ----------

# Para agrupar por múltiples columnas, simplemente pase las columnas como argumentos en groupBy()

df.groupBy("continent", "sub_region").sum("population").display()