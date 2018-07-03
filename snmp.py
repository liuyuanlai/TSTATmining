def get_throughput(node, port, ts=None, te=None):
    out_url = "http://graphite.es.net/snmp/west/%s/interface/%s/out?begin=%d&end=%d&calc=30"% (node, port, ts, te)
    in_url  = "http://graphite.es.net/snmp/west/%s/interface/%s/in?begin=%d&end=%d&calc=30" % (node, port, ts, te)
    r = requests.get(out_url).text
    res_out = json.loads(r)
    
    r = requests.get(in_url).text
    res_in = json.loads(r)

    return {'out': np.array(res_out['data']), 'in': np.array(res_in['data'])}


   # verified
alcf2nersc_esnet = (('star-cr5', 'to_anl_hpc_ip-a_v4v6'), ('chic-cr5', 'to_star-cr5_ip-a'), \
                    ('chic-cr5', 'to_star-cr5_ip-b'), ('kans-cr5', 'to_chic-cr5_ip-a'), \
                    ('denv-cr5', 'to_kans-cr5_ip-a'), ('sacr-cr5', 'to_denv-cr5_ip-a'), \
                    ('sunn-cr5', 'to_sacr-cr5_ip-a'), ('sunn-cr5', 'to_nersc_ip-a'))

nersc2alcf_esnet = (('sunn-cr5', 'to_nersc_ip-a'),    ('sacr-cr5', 'to_sunn-cr5_ip-a'),\
                    ('denv-cr5', 'to_sacr-cr5_ip-a'), ('kans-cr5', 'to_denv-cr5_ip-a'),\
                    ('chic-cr5', 'to_kans-cr5_ip-a'), ('star-cr5', 'to_chic-cr5_ip-a'), \
                    ('star-cr5', 'to_chic-cr5_ip-b'), ('star-cr5', 'to_anl_hpc_ip-a_v4v6'))


def get_path_throughput(path, ts, te):
    _ret = {}
    for node, port in path:
        _thro = get_throughput(node, port, ts, te)
        _ret[(node, port)] = _thro
    return _ret