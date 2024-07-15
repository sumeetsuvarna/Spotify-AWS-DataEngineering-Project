import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node album
album_node1720984932794 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-shubhi/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1720984932794")

# Script generated for node track
track_node1720984934193 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-shubhi/staging/track.csv"], "recurse": True}, transformation_ctx="track_node1720984934193")

# Script generated for node artist
artist_node1720984930413 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-shubhi/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1720984930413")

# Script generated for node Join Album and artist
JoinAlbumandartist_node1720985328710 = Join.apply(frame1=album_node1720984932794, frame2=artist_node1720984930413, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoinAlbumandartist_node1720985328710")

# Script generated for node Join with tracks
Joinwithtracks_node1720985533215 = Join.apply(frame1=JoinAlbumandartist_node1720985328710, frame2=track_node1720984934193, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtracks_node1720985533215")

# Script generated for node Drop Fields
DropFields_node1720985680859 = DropFields.apply(frame=Joinwithtracks_node1720985533215, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1720985680859")

# Script generated for node Destination
Destination_node1720985786760 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1720985680859, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-shubhi/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1720985786760")

job.commit()