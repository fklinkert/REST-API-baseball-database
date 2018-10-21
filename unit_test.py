import json
import SimpleBO

def test1():
    t = {"nameLast": ["Williams"], "nameFirst": ["Ted"]}
    result = SimpleBO.find_by_template("people", t, None)
    print("Result = ", json.dumps(result, indent=2))


test1()

def test2():
    t = {"nameLast": "Klinkert", "nameFirst": "Federico","playerID":"flinkert"}
    SimpleBO.insert("people", t)

test2()


def test3():
    t = {"nameLast": ["Klinkert"], "nameFirst": ["Federico"],"playerID":["flinkert"]}
    result = SimpleBO.find_by_template("people", t, None)
    print("Result = ", json.dumps(result, indent=2))

test3()

def test4():
    result = SimpleBO.get_primary_key("batting")
    print("Result = ", json.dumps(result, indent=2))
    
test4()
    
def test5():
    t = {'teamid':['BOS'], 'yearid':['2004']}
    result = SimpleBO.roster(t)
    print("Result = ", json.dumps(result, indent=2))

test5()

def test6():
    result = SimpleBO.find_by_primary_key('People','willite01')
    print("Result = ", json.dumps(result, indent=2))
    
test6()
    
def test7():
    result = SimpleBO.find_by_primary_key('People','smithbi04')
    print("Result = ", json.dumps(result, indent=2))
    SimpleBO.delete("people",'smithbi04')
    result = SimpleBO.find_by_primary_key('People','smithbi04')
    print("Result = ", json.dumps(result, indent=2))

test7()

def test8():
    t = {'throws' : 'C'}
    SimpleBO.update("People",'willite01',t)
    result = SimpleBO.find_by_primary_key('People','willite01')
    print("Result = ", json.dumps(result, indent=2))
    
test8()

def test9():
    result = SimpleBO.career_stats('willite01')
    print("Result = ", json.dumps(result, indent=2))
test9()  

def test10():  
    t = {'yearID':['1960']}
    result = SimpleBO.get_by_foreign_key('People','willite01',\
                                            'Batting', t)
    print("Result = ", json.dumps(result, indent=2))
test10()

def test11():    
    t = {'yearID':'2020','stint':'2','teamID':'BOS'}
    SimpleBO.insert_by_foreign_key('People','willite01', \
                                           'Batting',t)
    t = {'yearID':['2020']}
    result = SimpleBO.get_by_foreign_key('People','willite01',\
                                            'Batting', t)
    
    print("Result = ", json.dumps(result, indent=2))
    
test11()