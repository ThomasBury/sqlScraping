import os
import re
import itertools
import sql_metadata
# drc = "C:/Users/xtbury/Documents/Projects/CVI/SAS_codes_CVI/"

class ScrapSqlTab():
    """
    Class for performing table scraping in SAS files originally. Might work for other file format

    1. Walk the directory to find all files with the provided extension (default is ".sas")
    2. Look for start and end flags, for SAS, those are respectively, "proc sql" and "quit;"
    3. Extract the text between those flags
    4. Find the tables in the encapsulated SQL query by looking for keywords (select, from and join)
    5. Loop over all the instances in the file and loop over all files in the directory

    Parameters
    --------
        drc : string
            directory path, folder containing the files to scrap

        extension : string
            the file extension/format, default is extension=".sas"

        start_flag : string
            the start flag of the SQL query. If the file contains a blend of text or SQL+other language
            The default is start_flag="proc sql"

        end_flag : string
            the end flag of the SQL query. If the file contains a blend of text or SQL+other language
            The default is start_flag="quit;"

    Attributes
    --------
        self.schema_tab = schema_tab_dic
        self.query_tab = tab_dic
        self.tab = tab_list
        self.schema = schemas_list


    schema_tab : dict
        Dictionary of the found SQL schemas as key and correspond tables in values (list of each key)

    query_tab : dic
        Dictionary with query and table for the found procedure

    tab : list
        List of the found tabs, simple list not nested

    schema : list
        List of the found schema, simple list not nested

    Example
    --------

    drc = "C:/Users/xtbury/Documents/Projects/CVI/SAS_codes_CVI/"
    sasTab = ScrapSqlTab(drc=drc).get_tables_from_sas_scripts()
    sasTab.schema
    Out[1]: ['dwhint', 'work', 'wrkcvi', 'applctrl']
    sasTab.schema_tab
    Out[2]:
{'dwhint': ['ProdFam_NL',
  'accountDetail',
  'brokerlifeportfolio',
  'ComEv_NL',
  'clientContractDetail_EMP',
  'ClientDetail',
  'brokerresults',
  'brokeractivitypremiums',
  'brokeractivitynumbers',
  'clientContractDetail',
  'BrokerLifePortfolio',
  'ClientDetail_EMP',
  'ComEv_Broker_SP'],
 'work': ['clientSearchFilter',
  'ComEv_NL_Filtered',
  'accountSorted3',
  'accountsorted3',
  'foundAccounts',
  'filteredResults',
  'AccountFiltered',
  'accountsorted2',
  'clientMeasuresLife',
  'accountInfoFiltered',
  'clientDetailFilter',
  'client',
  'clientDetailFilter3',
  'brokers',
  'accountsFound',
  'Broker_ComEv_SP_fltr',
  'brokerSP_tmp',
  'Broker_ComEv_SP_fltr3',
  'filteredResults3',
  'accountFiltered',
  'accountSelected'],
 'wrkcvi': ['CVIFINAL'],
 'applctrl': ['applications']}

    """

    def __init__(self, drc, extension='sas', start_flag='proc sql', end_flag='quit;'):
        self.drc = drc
        self.extension = extension
        self.start_flag = start_flag
        self.end_flag = end_flag

    def get_filename(self):
        """
        Fetch the file names in the directory
        :return: list of file names ending with the defined extension
        """
        text_files = [f for f in os.listdir(self.drc) if f.endswith(self.extension)]
        return text_files

    def tables_in_query(self, sql_str):
        """
        Find the tables used in the sql query
        :param sql_str: the sql query found in the document
        :return:
           - tab_dic: dictionary with query string and corresponding table list
           - tab_list: list with the found table names
        """
        # # remove the /* */ comments
        # q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)
        # # remove whole line -- and # comments
        # lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
        # # remove trailing -- and # comments
        # q = " ".join([re.split("--|#", line)[0] for line in lines])
        # # split on blanks, parens and semicolons
        # tokens = re.split(r"[\s)(;]+", q)
        # # scan the tokens. if we see a FROM or JOIN, we set the get_next
        # # flag, and grab the next one (unless it's SELECT).
        # result = []
        # get_next = False
        # for tok in tokens:
        #     if get_next:
        #         if tok.lower() not in ["", "select"]:
        #             result.append(tok)
        #         get_next = False
        #     get_next = tok.lower() in ["from", "join"]
        result = sql_metadata.get_query_tables(re.sub(r'[;]', r' ', sql_str))
        tab_dic = {'query': sql_str, 'tables': [x.split('.')[1] if (len(x.split('.')) > 1) else x for x in result]}
            #result}
        tab_list = result
        return tab_dic, tab_list

    def get_tab(self, filepath):
        """
        Look for all queries in a document and extract the table names using
        tables_in_query()
        :param filepath: string, path of the file
        :return:
           - tab_dic: dictionay, for each sql procedure call, a list of queries and corresponding table names
           - tab_list: a list of all table names found in the document
        """
        f = open(filepath, "r")
        content = f.read()
        t = content.maketrans("\t\r", "  ")
        content = content.translate(t).splitlines()
        content = [x for x in content if re.sub('\s+', ' ', x)]
        # remove the /* */ comments
        q = [x for x in content if re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", x)]
        # q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", content)

        # remove whole line -- and # comments
        lines = [line for line in q if not re.match("^\s*(--|#|\*)", line)]
        # remove trailing -- and # comments
        q = " ".join([re.split("--|#", line)[0] for line in lines])
        pattern = self.start_flag+'(.+?)'+self.end_flag
        queries = re.findall(pattern, q)

        count = 0
        tab_dic = {}
        tab_list = []
        for query in queries:
            tab_d, tab_l = self.tables_in_query(query)
            tab_dic.update({'proc' + str(count): tab_d})
            tab_list.append(tab_l)
            count += 1
        return tab_dic, tab_list

    def get_tables_from_sas_scripts(self):
        """
        Loop over all documents in the specified directory
        :return:
          - schema_tab: dictionary, found schemas and a list of the corresponding table names
          - query_tab: dictionary, for each sql procedure, all the queries and the corresponding table names
          - tab: list, list of all table names
          - schema: list, list of all schemas
        """
        tab_dic = {}
        tab_list = []
        for dirpath, dirname, filename in os.walk(self.drc):  # Getting a list of the full paths of files
            for fname in filename:
                path = os.path.join(dirpath, fname)
                tab_d, tab_l = self.get_tab(path)
                tab_dic.update({fname: tab_d})
                tab_list.append(tab_l)
        tab_list = list(itertools.chain.from_iterable(tab_list))
        tab_list = list(itertools.chain.from_iterable(tab_list))
        tab_list = list(set(tab_list))
        schemas_list = set([x.split('.')[0] for x in tab_list if (len(x.split('.')) > 1)])
        schema_tab_dic = {}
        tab_no_schema = [x.split('.')[1] if (len(x.split('.')) > 1) else x for x in tab_list]
        for scheme in schemas_list:
            dum_list = [x.split('.')[1] for x in tab_list if x.split('.')[0] == scheme]
            schema_tab_dic.update({scheme: dum_list})
            tab_no_schema = list(set(tab_no_schema) - set(dum_list))

        self.schema_tab = schema_tab_dic
        self.query_tab = tab_dic
        self.tab = sorted(tab_list)
        self.schema = sorted(list(schemas_list))

        return self

