"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import glob
    import pandas as pd
    import os

    input_directory = "files/input/**/*.zip"
    output_directory = "files/output"

    def process_zip_file(zip_file, columns):
        data_frame = pd.read_csv(zip_file)

        data_frame["job"] = data_frame["job"].str.replace(".", "").str.replace("-", "_")
        data_frame["education"] = data_frame["education"].str.replace(".", "_")
        data_frame["education"] = data_frame["education"].apply(lambda x: pd.NA if x == "unknown" else x)
        data_frame["credit_default"] = data_frame["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
        data_frame["mortgage"] = data_frame["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
        data_frame["previous_outcome"] = data_frame["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
        data_frame["campaign_outcome"] = data_frame["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
        data_frame["last_contact_date"] = pd.to_datetime(data_frame["day"].astype(str) + '-' + data_frame["month"] + "-2022", format="%d-%b-%Y")

        client_info = data_frame[columns[:7]].values.tolist()
        campaign_info = data_frame[columns[0:1]].join(data_frame[columns[7:13]]).values.tolist()
        economic_indicators = data_frame[columns[0:1]].join(data_frame[columns[13:]]).values.tolist()

        return client_info, campaign_info, economic_indicators

    def process_all_zip_files(input_directory, columns):
        zip_files = glob.glob(input_directory, recursive=True)
        
        all_client_info = []
        all_campaign_info = []
        all_economic_indicators = []

        for zip_file in zip_files:
            client_info, campaign_info, economic_indicators = process_zip_file(zip_file, columns)
            all_client_info.extend(client_info)
            all_campaign_info.extend(campaign_info)
            all_economic_indicators.extend(economic_indicators)

        return all_client_info, all_campaign_info, all_economic_indicators

    def save_dataframes_to_csv(client_info, campaign_info, economic_indicators, columns, output_directory):
        client_df = pd.DataFrame(client_info, columns=columns[:7])
        campaign_df = pd.DataFrame(campaign_info, columns=[columns[0]] + columns[7:13])
        economic_df = pd.DataFrame(economic_indicators, columns=[columns[0]] + columns[13:])

        os.makedirs(output_directory, exist_ok=True)

        client_df.to_csv(os.path.join(output_directory, "client.csv"), index=False)
        campaign_df.to_csv(os.path.join(output_directory, "campaign.csv"), index=False)
        economic_df.to_csv(os.path.join(output_directory, "economic.csv"), index=False)


    columns = ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage", "number_contacts", "contact_duration",
            "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "last_contact_date", "cons_price_idx", "euribor_three_months"]

    client_info, campaign_info, economic_indicators = process_all_zip_files(input_directory, columns)
    save_dataframes_to_csv(client_info, campaign_info, economic_indicators, columns, output_directory)




if __name__ == "__main__":
    clean_campaign_data()
