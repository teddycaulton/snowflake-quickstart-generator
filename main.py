import yaml
import streamlit as st

st.title("Snowflake Quickstart Generator")
st.subheader("This application lets you stand up the skeleton for a full Snowflake ETL architecture.  Fill out the config template to assign the desired environments, layers and role names for raw ingestion and ETL transformations.")

st.write("Download the template below and fill out the fields included, please note: order matters and fields are case sensitive")
with open('config_template.yaml', 'rb') as file:
    btn = st.download_button(label = "Sample Template", data = file, file_name = "config_template.yaml")

uploaded_yaml = st.file_uploader("Upload Config File")
if uploaded_yaml is not None:
    config = yaml.safe_load(uploaded_yaml)

    environments = config['environments']
    layers = config['layers']
    raw_ingestion_role = config['raw_ingestion_role']
    transformation_role = config['transformation_role']
    raw_ingestion_wh = config['raw_ingestion_wh']
    transformation_wh = config['transformation_wh']
    raw_ingestion_wh_size = config['raw_ingestion_wh_size']
    transformation_wh_size = config['transformation_wh_size']

    role_list = [raw_ingestion_role, transformation_role]
    wh_list = [(raw_ingestion_wh, raw_ingestion_wh_size), (transformation_wh, transformation_wh_size)]

    script = ""
    script += "USE ROLE SYSADMIN;\n\n"
    for wh, size in wh_list:
        script += f"CREATE WAREHOUSE IF NOT EXISTS {wh} WAREHOUSE_SIZE = {size};\n"
    script += "\n"

    for env in environments:
        script += f"---- {env} environment setup\n"
        script += f"CREATE DATABASE IF NOT EXISTS {env};\n---- schema setup\n"
        for layer in layers:
            script += f"CREATE SCHEMA IF NOT EXISTS {env}.{layer};\n"
        script += "\n"

    script += "USE ROLE USERADMIN;\n"
    for role in role_list:
        script += f"CREATE ROLE IF NOT EXISTS {role};\n"
    script += "USE ROLE SECURITYADMIN;\nGRANT MANAGE GRANTS ON ACCOUNT TO ROLE SYSADMIN;\n\nUSE ROLE SYSADMIN;\n"

    for env in environments:
        script += f"----database and schema grants for {env}\n"
        script += f"GRANT USAGE ON DATABASE {env} TO ROLE {raw_ingestion_role};\n"
        script += f"GRANT USAGE ON DATABASE {env} TO ROLE {transformation_role};\n"
        script += f"-- grants for {env}.{layers[0]}\nGRANT USAGE ON SCHEMA {env}.{layers[0]} TO ROLE {raw_ingestion_role};\nGRANT CREATE STAGE ON SCHEMA {env}.{layers[0]} TO ROLE {raw_ingestion_role};\nGRANT SELECT, INSERT, UPDATE, TRUNCATE ON FUTURE TABLES IN SCHEMA {env}.{layers[0]} TO ROLE {raw_ingestion_role};\nGRANT USAGE ON SCHEMA {env}.{layers[0]} TO ROLE {transformation_role};\nGRANT SELECT ON FUTURE TABLES IN SCHEMA {env}.{layers[0]} TO ROLE {transformation_role};\n"
        for layer in layers[1:]:
            script += f"-- grants for {env}.{layer}\nGRANT USAGE ON SCHEMA {env}.{layer} TO ROLE {transformation_role};\nGRANT CREATE STAGE ON SCHEMA {env}.{layer} TO ROLE {transformation_role};\nGRANT SELECT, INSERT, UPDATE, TRUNCATE ON FUTURE TABLES IN SCHEMA {env}.{layer} TO ROLE {transformation_role};\n"
        script += f"-- grants for {env}.PUBLIC\nGRANT USAGE ON SCHEMA {env}.PUBLIC TO ROLE {raw_ingestion_role};\n"
        script += "\n" 
    st.code(script, language = "sql")
