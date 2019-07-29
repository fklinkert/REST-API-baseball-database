import pymysql

cnx = pymysql.connect(host='localhost',
                              user='root',
                              password='password',
                              db='lahman2017raw',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)


def run_q(q, args, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result

def template_to_where_clause(t):
    s = ""

    if t is None:
        return s

    for (k, v) in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v[0] + "'"

    if s != "":
        s = " WHERE " + s;

    return s


def find_by_template(table, template, fields=None):
    
    offset = '0'
    limit = '10'
    
    if 'offset' in template.keys():
        offset = template['offset'][0]
        template.pop('offset', None)
         
    if 'limit' in template.keys():
        limit = template['limit'][0]
        template.pop('limit', None)
       
    
    wc = template_to_where_clause(template)
    if fields == None:
        select = "*"
    else:
        select = ", ".join(fields)

    q = "SELECT " + select + " from " + table + " " + wc  + \
                            " LIMIT " + offset + ", " + limit
    
    result = run_q(q, None, True)
    
    if len(result) == int(limit):
        links = pagination_links(table,template,select,offset,limit)
        f_result = {}
        f_result["data"] = result
        f_result["links"] = links
    else:
        f_result = result
        
    
    return f_result


def insert(table, row):
    keys = list(row.keys())
    keys = " (" + ', '.join(keys) +") "
    values = list(row.values())
    v = "("
    for i in range(len(values)):
        v = v + "'" + str(values[i]) + "'"
        if i != len(values)-1:
            v = v + ", "
    v = v + ")"
    q = "INSERT INTO " + table + keys + "VALUES " + v   
    
    run_q(q,None,False) 
    
    pass
     

def get_primary_key(resource):
    
    q = "SELECT table_name, column_name, ordinal_position, " \
            + "constraint_schema, constraint_name FROM " \
            + "INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE " \
            + "table_name=%s and table_schema=%s and constraint_name='PRIMARY'"
    db_schema='lahman2017raw'
    result = run_q(q, (resource,db_schema),True)
    key_def = [r['column_name'] for r in result]
        
    return key_def

def get_template_by_primary_key(resource, primary_key):
    keys = get_primary_key(resource)
    PK = []
    l = []
    S = ""
    for i in range(len(primary_key)):
        if primary_key[i] == "_":
            l.append(S)
            PK.append(l)
            l = []
            S = ""
        else:
            S += primary_key[i]
    l.append(S)       
    PK.append(l)
    t = {}
    for i in range(len(keys)):
        t[keys[i]] = PK[i]
    return t

def find_by_primary_key(table, primary_key, fields=None):
    t = get_template_by_primary_key(table, primary_key)
    result = find_by_template(table, t, fields)
    return result

def delete(table, primary_key):
    t = get_template_by_primary_key(table, primary_key)
    w = template_to_where_clause(t)
    q = "DELETE FROM " + table + " " + w + ";"
    run_q(q,None,False)
    pass

def update_put_clause(t):
    s = ""

    if t is None:
        return s

    for (k, v) in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v + "'"

    return s

def update(table, primary_key, body):
    t = get_template_by_primary_key(table, primary_key)
    upd = update_put_clause(body)
    w = template_to_where_clause(t)
    q = "UPDATE " + table + " SET " + upd + w
    run_q(q,None,False)
    pass

def get_by_foreign_key(table,pk,F_table, in_args, fields=None):
    t = get_template_by_primary_key(table, pk)
    in_args.update(t)
    return find_by_template(F_table,in_args,fields)
    
def insert_by_foreign_key(table, pk, F_table, body):
    t = get_template_by_primary_key(table, pk)
    for key in t:
        t[key] = t[key][0]
    body.update(t)
    insert(F_table,body)
    pass

def teammates(playerID):
    
    q = "SELECT distinct \
	(select people.playerID from people where people.playerID = '"+playerID+"') as playerID,\
	appearances2.playerid as teammateID, \
	min(appearances2.yearid) as first_year, \
	max(appearances2.yearid) as last_year,\
    count(appearances1.teamid=appearances2.teamid and \
    appearances1.yearid=appearances2.yearid) as seasons \
    from appearances as appearances1  left join appearances as \
    appearances2  on appearances1.teamid=appearances2.teamid and \
    appearances1.yearid=appearances2.yearid\
    where appearances1.playerid='" + playerID + "'\
	GROUP BY teammateID\
	ORDER BY teammateID ASC;"
    
    result = run_q(q,None,True)
    return result

def career_stats(playerID):
    
    q = "SELECT DISTINCT\
    People.playerID as playerid, \
    Batting.teamID, Batting.yearID, G_all, h as hits, AB as ABs,\
    A as assits, E as `errors`\
    from People \
    join appearances on people.playerid = appearances.playerid\
    join batting on People.playerid = Batting.playerid and \
    Batting.teamid = Appearances.teamid and Batting.yearid = Appearances.yearid\
    join fielding on people.playerid = fielding.playerid and \
    Batting.teamid = fielding.teamid and Batting.yearid = fielding.yearid \
    WHERE People.playerID='" + playerID+ "';"
    
    result = run_q(q,None,True)
    return result

def roster(in_args):
    
    teamID = in_args['teamid'][0]
    yearID = in_args['yearid'][0]
    
    q = "SELECT DISTINCT Appearances.playerID, " + \
    "(SELECT People.nameFirst FROM People WHERE People.playerID=Appearances.playerID)\
    as first_name, \
    (SELECT People.nameLast FROM People WHERE People.playerID=Appearances.playerID)\
    as last_name, appearances.teamID, appearances.yearID,G_all, h as hits, AB as ABs,\
    A as attemps, E as `errors`\
    FROM Appearances \
    join batting on appearances.playerid = Batting.playerid and appearances.yearID = Batting.yearID\
    and appearances.teamID = Batting.teamID\
    join fielding on appearances.playerid = fielding.playerid and appearances.yearID = fielding.yearID\
    and appearances.teamID = fielding.teamID\
    WHERE appearances.teamID='" + teamID + "' and appearances.yearID='" + yearID + "'\
    ORDER BY appearances.playerID ASC;"
    
    result = run_q(q,None,True)
    return result

def pagination_links(resource,template,select,offset,limit):
    base = "/api/" + resource + "?"
    s = ""
    if template is not None:
        for (k, v) in template.items():
            s += k + "=" + v[0] + ","

    base += s
    base = base[:-1]
    
    if select != "*":
        f = "&fields=" + select
        base += f
    
    links = {}
    
    offset = int(offset)
    limit = int(limit)
    if offset != 0:
        p_offset = str(offset - limit)
        links['previous'] = base + "&offset=" + p_offset + "&limit=" + str(limit)
    links['current'] =  base + "&offset=" + str(offset) + "&limit=" + str(limit)  
    f_offset = str(offset + limit)
    links['next'] =  base + "&offset=" + f_offset + "&limit=" + str(limit)
    
    return links

