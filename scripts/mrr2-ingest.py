import sage_data_client
import requests
import os

def readtofile(uurl, ff, username, password):
    r = requests.get(uurl, auth=(username,password))
    if r.status_code == 200:
        print('Downloading %s' % uurl[-18:])
        with open(ff, 'wb') as out:
            for bits in r.iter_content():
                out.write(bits)

if __name__ == "__main__":
    df = sage_data_client.query(
                start="-30d",
                filter={"vsn": "W08D", "name": "upload", "task": "mrrpro",
                        }).set_index("timestamp")

    site = "neiu"
    instrument = "mrrpro"
    level = "a1"

    file_list = list(df.value.values)

    cur_time = ""
    for f in file_list:
        time_stamp = f[-18:]
        if cur_time == time_stamp:
            file_list.remove(f)
        else:
            cur_time = time_stamp
            
    username = 'jenny'
    password = '8675309'

    if not os.path.exists('mrr_data'):
        os.mkdir('mrr_data')


    temp_file_list = []
    for f in file_list:
        name = f[-18:].replace("_", "-")
        out_name = '/data/datastream/neiu/neiu-mrrpro-a1/%s-%s-%s-%s' % (site, instrument, level, name)
        if not os.path.exists(out_name):
            print(f)
            readtofile(f, out_name, username, password)
        if not out_name in temp_file_list:
            temp_file_list.append(out_name)
    

