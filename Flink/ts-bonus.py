from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, DataTypes, Schema


#Zmena na streaming mod
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

td = TableDescriptor.for_connector('filesystem')\
    .schema(Schema.new_builder()
    .column('custID', DataTypes.INT())
    .column('prodID', DataTypes.INT())
    .column('price', DataTypes.FLOAT())
    .build())\
    .option('path', "/files/customer-orders.csv")\
    .format('csv')\
    .build()

table_env.create_temporary_table('source', td)

table = table_env.from_path('source')

result = table.group_by(table.custID)\
    .select(table.custID, table.price.sum.alias("total_spent"))

otd = TableDescriptor.for_connector('print')\
    .schema(Schema.new_builder()
    .column('custID', DataTypes.INT())
    .column('total_spent', DataTypes.DECIMAL(10, 2))
    .build())\
    .build()

table_env.create_temporary_table('sink', otd)

result.execute_insert("sink").wait()

# with table.execute().collect() as results:
#     for r in results:
#         print(r)
