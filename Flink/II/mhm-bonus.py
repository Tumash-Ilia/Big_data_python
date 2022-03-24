from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, DataTypes, Schema, FormatDescriptor

#Vytvoreni table environment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

#Vytvoreni tabulek a nacteni dat
#Hodnoceni filmu
td = TableDescriptor.for_connector('filesystem') \
    .schema(Schema.new_builder()
            .column('userID', DataTypes.INT())
            .column('movieID', DataTypes.INT())
            .column('rating', DataTypes.FLOAT())
            .column('timestamp', DataTypes.STRING())
            .build()) \
    .option('path', '/files/u.data') \
    .format(FormatDescriptor.for_format("csv").option('field-delimiter', "\t").build()) \
    .build()

table_env.create_temporary_table('ratings', td)
ratings = table_env.from_path('ratings')
#Informace k filmum
td2 = TableDescriptor.for_connector('filesystem') \
    .schema(Schema.new_builder()
            .column('id', DataTypes.INT())
            .column('name', DataTypes.STRING())
            .build()) \
    .option('path', '/files/u-mod.item') \
    .format(FormatDescriptor.for_format('csv').option('field-delimiter', '|').build()) \
    .build()

table_env.create_temporary_table('movies', td2)
movies = table_env.from_path('movies')

#Dotazy

#Filtrovani flmu s nejlepsim hodnoceni

#Celkovy pocet hodnoceni
rait_all = ratings.group_by(ratings.movieID) \
    .select(ratings.movieID, ratings.rating.count.alias('all_rating'))

#Pocet honoceni = 5
ratings = ratings.filter(ratings.rating == 5)
rait5 = ratings.group_by(ratings.movieID) \
    .select(ratings.movieID.alias('mid'), ratings.rating.count.alias('best_rating'))

#Vypocet pomeru
results = rait_all.join(rait5)\
    .where(rait_all.movieID == rait5.mid)\
    .add_columns(((rait5.best_rating * 1.0) / rait_all.all_rating).alias('ratio'))


sorted = results.order_by(results.ratio.desc).fetch(20)

#Propojeni s nazvem filmu
joined = sorted.join(movies) \
    .where(sorted.movieID == movies.id) \
    .select(movies.name, sorted.ratio) \
    .order_by(sorted.ratio.desc)

#Nadefinovani vystupni tabulky
otd = TableDescriptor.for_connector('print') \
    .schema(Schema.new_builder()
            .column('movieName', DataTypes.STRING())
            .column('popularity', DataTypes.DECIMAL(10, 2))
            .build()) \
    .build()

table_env.create_temporary_table('sink', otd)
joined.execute_insert('sink').wait()
