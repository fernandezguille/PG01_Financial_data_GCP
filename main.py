from google.cloud import bigquery

project_name = 'consultora-01'
bigquery_dataset = 'Datawarehouse'

schemas_id = {'Microsoft': [
                bigquery.SchemaField("Fecha", "DATE"),
                bigquery.SchemaField("Apertura", "FLOAT"),
                bigquery.SchemaField("Maximo", "FLOAT"),
                bigquery.SchemaField("Minimo", "FLOAT"),
                bigquery.SchemaField("Cierre", "FLOAT"),
                bigquery.SchemaField("Ajuste de cierre", "FLOAT"),
                bigquery.SchemaField("Volumen", "INTEGER"),],
                'Apple':[
                bigquery.SchemaField("Fecha", "DATE"),
                bigquery.SchemaField("Apertura", "FLOAT"),
                bigquery.SchemaField("Maximo", "FLOAT"),
                bigquery.SchemaField("Minimo", "FLOAT"),
                bigquery.SchemaField("Cierre", "FLOAT"),
                bigquery.SchemaField("Ajuste de cierre", "FLOAT"),
                bigquery.SchemaField("Volumen", "INTEGER"),],
                'Tesla':[
                bigquery.SchemaField("Apertura", "FLOAT"),
                bigquery.SchemaField("Maximo", "FLOAT"),
                bigquery.SchemaField("Minimo", "FLOAT"),
                bigquery.SchemaField("Cierre", "FLOAT"),
                bigquery.SchemaField("Ajuste de cierre", "FLOAT"),
                bigquery.SchemaField("Volumen", "INTEGER"),],
                'Meta':[
                bigquery.SchemaField("Fecha", "DATE"),
                bigquery.SchemaField("Apertura", "FLOAT"),
                bigquery.SchemaField("Maximo", "FLOAT"),
                bigquery.SchemaField("Minimo", "FLOAT"),
                bigquery.SchemaField("Cierre", "FLOAT"),
                bigquery.SchemaField("Ajuste de cierre", "FLOAT"),
                bigquery.SchemaField("Volumen", "INTEGER"),]
                }

def mover_dataset_x(event,context):
    file = event
    file_name = file['name']   #se saca el nombre del archivo nuevo en el bucket ETL
    table_name = file_name.split(".")[0] # nombre del archivo sin el .csv
    print(f"Se detectó que se subió el archivo {file_name} en el bucket {file['bucket']}.")
    source_bucket_name = file['bucket'] #nombre del bucket donde esta el archivo

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    source_path = "gs://"+source_bucket_name+"/"+file_name # ruta al archivo cargado en el bucket de stage
    table_id = project_name + "." + bigquery_dataset + "." + table_name 
    # table_id - ruta a la tabla a carga en big query: "nombre_del_proyecto"."nombre_de_la_base_de_datos"."nombre de la tabla"
    # cambiar nombre del proyecto y nombre de la base de datos en bigquery arriba (al inicio)  

    job_config = bigquery.LoadJobConfig(
        schema = schemas_id[table_name],
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    #poner ubicación de archivo customers
    uri = source_path

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))