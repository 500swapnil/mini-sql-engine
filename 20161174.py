import sqlparse
import os, sys
import csv
import re
import numbers

query = sys.argv[1]
statement = sqlparse.parse(query)[0].tokens
metaData = {}

def readMetadata():
    f = open('files/metadata.txt','r')
    tableStart = False
    for line in f:
        lineList = line.strip()
        if lineList == "<begin_table>":
            tableStart = True
            
        elif tableStart == True:
            tableName = lineList
            metaData[tableName] = []
            tableStart = False
            
        elif lineList == '<end_table>':
            pass
        
        else:
            metaData[tableName].append(lineList)

    for i in metaData:
        metaData[i] = filter(None, metaData[i])



def executeQuery(iden):
    if len(iden) not in [4,5]:
        print"ERROR:invalid query"
        return
    if len(iden) == 5:
        attributes = re.sub(ur"[\,]",' ',iden[1]).split()
        tables = re.sub(ur"[\,]",' ',iden[3]).split()
        if len(tables)>1:
            print "ERROR:Cannot access more than 1 table"
            return
        where = iden[4].split()
        filePath = 'files/'+ iden[3] + '.csv'
        f = open(filePath,'rb')
        reader = csv.reader(f)
        columnNumbers = []
        values = []
        f = open(filePath,'rb')
        reader = csv.reader(f)
        for row in reader:
                values.append(row)
        

        if(len(tables)==1):
            
            table=tables[0]
            if len(where)>2:
                cond1 = [False]*len(values)
                if '<=' in where[1]:
                    lequal = where[1].split('<=')
                    column = lequal[0]
                    value = lequal[1]
                    op = '<='
                elif '>=' in where[1]:
                    gequal = where[1].split('>=')
                    column = gequal[0]
                    value = gequal[1]
                    op = '>='
                elif '<' in where[1]:
                    less = where[1].split('<')
                    column = less[0]
                    value = less[1]
                    op = '<'
                elif '>' in where[1]:
                    great = where[1].split('>')
                    column = great[0]
                    value = great[1]
                    op = '>'
                elif '=' in where[1]:
                    equal = where[1].split('=')
                    column = equal[0]
                    value = equal[1]
                    op = '='
                else:
                    print"ERROR: Invalid Condition"
                    return
                i = 0
                f = open(filePath,'rb')
                reader = csv.reader(f)
                for row in reader:
                    check = metaData[table].index(column)
                    if op == '=' and int(str(row[check])) == int(value):
                        cond1[i] = True
                    elif op == '<=' and int(str(row[check])) <= int(value):
                        cond1[i] = True
                    elif op == '>=' and int(str(row[check])) >= int(value):
                        cond1[i] = True
                    elif op == '<' and int(str(row[check])) < int(value):
                        cond1[i] = True
                    elif op == '>' and int(str(row[check])) > int(value):
                        cond1[i] = True
                    i += 1
                if len(where) == 3:
                    print"ERROR: Invalid Condition"
                    return
                if len(where) == 4:
                    cond2 = [False]*len(values)
                    if '<=' in where[3]:
                        lequal = where[3].split('<=')
                        column = lequal[0]
                        value = lequal[1]
                        op = '<='
                    elif '>=' in where[3]:
                        gequal = where[3].split('>=')
                        column = gequal[0]
                        value = gequal[1]
                        op = '>='
                    elif '<' in where[3]:
                        less = where[3].split('<')
                        column = less[0]
                        value = less[1]
                        op = '<'
                    elif '>' in where[3]:
                        great = where[3].split('>')
                        column = great[0]
                        value = great[1]
                        op = '>'
                    elif '=' in where[3]:
                        equal = where[3].split('=')
                        column = equal[0]
                        value = equal[1]
                        op = '='
                    else:
                        print"ERROR: Invalid Condition"
                        return
                    i = 0
                    f = open(filePath,'rb')
                    reader = csv.reader(f)
                    for row in reader:
                        check = metaData[table].index(column)
                        if op == '=' and int(str(row[check])) == int(value):
                            cond2[i] = True
                        elif op == '<=' and int(str(row[check])) <= int(value):
                            cond2[i] = True
                        elif op == '>=' and int(str(row[check])) >= int(value):
                            cond2[i] = True
                        elif op == '<' and int(str(row[check])) < int(value):
                            cond2[i] = True
                        elif op == '>' and int(str(row[check])) > int(value):
                            cond2[i] = True
                        i += 1
                else:
                    cond2 = [True]*len(values)    
                if attributes[0] == '*':
                    attributes = metaData[table]

                for at in attributes:
                    print at+'\t',
                    if at not in metaData[table]:
                        print"ERROR: attribute doesn't exist"
                        return
                print '\n'

                if len(where) == 4:
                    join = where[2]
                else:
                    join = 'and'
                
                i = 0
                f = open(filePath,'rb')
                reader = csv.reader(f)
                for row in reader:
                    check = metaData[table].index(column)
                    
                    if join == 'and' and cond1[i] and cond2[i]:
                        for at in attributes:
                            j = metaData[table].index(at)
                            print row[j]+'\t',
                        print '\n',
                    elif join == 'or' and (cond1[i] or cond2[i]):
                        for at in attributes:
                            j = metaData[table].index(at)
                            print row[j]+'\t',
                        print '\n',
                    
                    i+=1
                    
                    
        else:
            pass
        f.close()
        return
    
    attributeName = iden[1]
    tableName = iden[3]
    attr = re.sub(ur"[\(\)]",' ',attributeName).split()
    aggregate = False
    if attr[0] in ['max', 'min', 'sum', 'avg', 'distinct']:
        aggregate = True

    if tableName in metaData.keys():
        filePath = 'files/'+ tableName + '.csv'
        f = open(filePath,'rb')
        
        reader = csv.reader(f)

        if attr[0] == '*':
            for i in metaData[tableName]:
                print i+'\t',
            print '\n'
            for row in reader:
                for i in row:
                    print i+'\t',
                print '\n'

        
        elif not aggregate:
            tables = re.sub(ur"[\,]",' ',tableName).split()
            attributes = re.sub(ur"[\,]",' ',attributeName).split()
            
            if len(tables)==1:
                table = tables[0]

                for i in attributes:
                    print i+'\t',
                    if i not in metaData[table]:
                        print "ERROR: attribute doesn't exist"
                        return
                print '\n'
                for row in reader:
                    for i in attributes:
                        j = metaData[table].index(i)
                        print row[j]+'\t',
                    print '\n',

        else:
            attribute = attr[1]
            columnNumber = 0
            values=[]
            agg = attr[0]
            
            if attribute not in metaData[tableName]:
                print "ERROR: attribute does not exist"
                return
            else:
                columnNumber = metaData[tableName].index(attribute)

            
            for row in reader:
                values.append(int(row[columnNumber]))

            print attributeName
            if agg == 'distinct':
                for i in list(set(values)):
                    print i
            elif agg == 'sum':
                print sum(values)
            elif agg == 'max':
                print max(values)
            elif agg == 'avg':
                print sum(values)/len(values)
            elif agg == 'min':
                print min(values)
            

        f.close()
    else:
        print "ERROR: table does not exist"
        

def parseStatement(statement):
    sType = sqlparse.sql.Statement(statement).get_type()
    sIdentifiers = sqlparse.sql.IdentifierList(statement).get_identifiers()
    
    iden = []
    for i in sIdentifiers:
        iden.append(str(i))
    
    executeQuery(iden)

readMetadata()
parseStatement(statement)
