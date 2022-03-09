from http.client import NETWORK_AUTHENTICATION_REQUIRED
import json
import pandas as pd
import xlsxwriter

class ParseLineage():

    def __init__(self, jlineage):
        self.jlineage = jlineage
        self.agg = 0

    def loadLineage(self):
        self.lineage = json.loads(open(self.jlineage).read())
        return True

    def getObjects(self):
        writes = self.lineage['executionPlan']['operations']['write']
        try:
            reads = self.lineage['executionPlan']['operations']['read']
        except:
            reads = self.lineage['executionPlan']['operations']['reads']
        others = self.lineage['executionPlan']['operations']['other']
        attribs = self.lineage['executionPlan']['attributes']
        functions = self.lineage['executionPlan']['expressions']['functions']
        self.writes = writes
        self.reads = reads
        self.others = others 
        self.attribs = attribs
        self.functions = functions
        return True

    def getName(self, attr):
        for attrib in self.attribs:
            if attrib['id'] == attr:
                return attrib['name']

    def getDatatypes(self, attr):
        for attrib in self.attribs:
            if attrib['id'] == attr:
                dtype = attrib['dataType']
                for type in self.lineage['executionPlan']['extraInfo']['dataTypes']:
                    if type['id'] == dtype:
                        return type['name']

    def getDatatype(self, dtype):
        for type in self.lineage['executionPlan']['extraInfo']['dataTypes']:
            if type['id'] == dtype:
                return type['name']

    def getOriginalName(self, attr):
        for attrib in self.attribs:
            if attrib['id'] == attr:
                expr = attrib['childRefs'][0]['__exprId']
                for func in self.functions:
                    if func['id'] == expr:
                        if '__attrId' in func['childRefs'][0]:
                            attr = func['childRefs'][0]['__attrId']
                            name = self.getName(attr)
                        else:
                            expr = func['childRefs'][0]['__exprId']
                            name, attr = self.getExprName(expr) 
                        return name, attr

    def getExprName(self, expr):
        for func in self.functions:
            if func['id'] == expr:
                if '__attrId' in func['childRefs'][0]:
                    self.attr = func['childRefs'][0]['__attrId']
                    self.name = self.getName(attr)
                else:
                    exprid = func['childRefs'][0]['__exprId']
                    attr = self.findAttr(exprid)
        return self.name, self.attr
    
    def findAttr(self, exprid):
        for func in self.functions:
            if func['id'] == exprid:
                if '__attrId' in func['childRefs'][0]:
                    self.attr = func['childRefs'][0]['__attrId']
                    self.name = self.getName(self.attr)
                else:
                    exprid = func['childRefs'][0]['__exprId']
                    self.findAttr(exprid)
        return self.name, self.attr

    def getReads(self):
        names = []
        splineid = []
        datatype = []
        source = []
        for read in self.reads:
            for attr in read['output']:
                names.append(self.getName(attr))
                splineid.append(attr)
                datatype.append(self.getDatatypes(attr))
                source.append(read['inputSources'][0])
        self.readNames = names
        self.readSplineId = splineid
        self.readDataType = datatype
        self.readSource = source
        return True

    def getWrite(self):
        print('in getWrite')
        names = []
        splineid = []
        datatype = []
        dest = []
        write = self.writes
        child = write['childIds']
        for other in self.others:
            if other['id'] == child[0]:
                for attr in other['output']:
                    dname = {}
                    did = {}
                    ddtype = {}
                    ddest = {}
                    dname['newName'] = self.getName(attr)
                    did['newID'] = attr
                    ddtype['newDatatype'] = self.getDatatypes(attr)
                    if attr not in self.readSplineId:
                        origName, origId = self.getOriginalName(attr)
                        dname['originalName'] = origName
                        did['originalID'] = origId
                    ddest['outputSource'] = write['outputSource']    
                    names.append(dname)
                    splineid.append(did)
                    datatype.append(ddtype)
                    dest.append(ddest)       
        self.outputNames = names
        self.outputId = splineid
        self.outputDataType = datatype
        self.outputDest = dest
        return True

    def getJoinDetails(self, other):
        if not hasattr(ParseLineage, 'join_details'):
            self.join_details = []
        joinstuff = {}
        sources = other['childIds']
        print('sources: ', sources)
        for id in sources:
            join_deets = {}
            print('id: ', id)
            for read in self.reads:
                if read['id'] == id:
                    print('read_id: ', read['id'])
                    print('read source: ', read['inputSources'][0])
                    join_deets['inputSources'] = read['inputSources'][0]
                else:
                    for oth in self.others:
                        if oth['id'] == id:
                            print('oth_id: ', oth['id'])
                            inpId = oth['childIds'][0]
                            print('inpId: ', inpId)
                            for read in self.reads:
                                if read['id'] == inpId:
                                    print(read['inputSources'][0])
                                    join_deets['inputSources'] = read['inputSources'][0]
            self.join_details.append(join_deets)    
        exprId = other['params']['condition']['__exprId']
        for func in self.functions:
            if func['id'] == exprId:
                matchcols = []
                matchcols.append(func['childRefs'][0]['__attrId'])
                matchcols.append(func['childRefs'][1]['__attrId'])
                match_cols = []
                for attr in matchcols:
                    name = self.getName(attr)
                    match_cols.append(name)
                joinstuff['match_cols'] = match_cols
                joinstuff['expr_type'] = func['name']
        self.join_details.append(joinstuff)
        print(self.join_details)
        return self.join_details

    def getAggCols(self, other):    
        aggCols = other['params']['aggregateExpressions']
        agg_cols = []
        for item in aggCols:
            if '__attrId' in item:
                colname = self.getName(item['__attrId'])
                agg_cols.append(colname)
            else: 
                exprId = item['__exprId']
                for func in self.functions:
                    if func['id'] == exprId:
                        colname = func['name']
                        agg_cols.append(colname)
        return agg_cols
        
    def getAggActions(self, expr):
        actions = []
        for func in self.functions:
            if func['id'] == expr:
                actions.append(func['extra']['simpleClassName'])
                expr2 = func['childRefs'][0]['__exprId']
        for func in self.functions:
            if func['id'] == expr2:
                actions.append(func['name'])
                expr3 = func['childRefs'][0]['__exprId']
        for func in self.functions:
            if func['id'] == expr3:
                actions.append(func['name'])
                expr4 = func['childRefs'][0]['__exprId']
        for func in self.functions:
            if func['id'] == expr4:
                actions.append(func['name'])
                attrib = func['childRefs'][0]['__attrId']
        for column in self.attribs:
            if column['id'] == attrib:
                name = []
                name.append(attrib)
                name.append(column['name'])
        return name, actions 

    def getAggregateDetails(self, other):
        if not hasattr(ParseLineage, 'aggregate_details'):
            self.aggregate_details = []
        agg_deets = {}
        sourceId = other['childIds'][0]
        for read in self.reads:
            if read['id'] == sourceId:
                agg_deets['source'] = read['inputSources'][0]
        outPutAttr = other['output']
        agg_cols = []
        for attr in outPutAttr:
            name = self.getName(attr)
            agg_cols.append(name)
        agg_deets['AggregateColumns'] = agg_cols 
        groupCol = other['params']['groupingExpressions'][0]['__attrId']
        groupColName = self.getName(groupCol)
        agg_deets['groupColumn'] = groupColName
        name, actions = self.getAggActions(other['params']['aggregateExpressions'][1]['__exprId']) 
        agg_deets['actions'] = actions
        agg_deets['originalName'] = name
        self.aggregate_details.append(agg_deets)
        return self.aggregate_details

    def getAggAliasName(self, expr):
        actions = []
        for func in self.functions:
            if func['id'] == expr:
                expr2 = func['childRefs'][0]['__exprId']
        for func in self.functions:
            if func['id'] == expr2:
                expr3 = func['childRefs'][0]['__exprId']
        for func in self.functions:
            if func['id'] == expr3:
                attrib = func['childRefs'][0]['__attrId']
        for column in self.attribs:
            if column['id'] == attrib:
                name = []
                name.append(attrib)
                name.append(column['name'])
        return name 

    def getAliasNewId(self, extExprId):
        ext = extExprId.split('(')
        attr = ext[1].split(',')[0]
        return attr

    def getAliasDetails(self, func):
        if not hasattr(ParseLineage, 'alias_details'):
            self.alias_details = []
        alias_deet = {}
        alias_deet['dataType'] = self.getDatatype(func['dataType'])
        if self.agg == 1:
            name = self.getAggAliasName(func['childRefs'][0]['__exprId'])
            alias_deet['original_name'] = name    
        else:
            alias_deet['originalName'] = self.getName(func['childRefs'][0]['__attrId'])
            alias_deet['originalID'] = func['childRefs'][0]['__attrId']
        alias_deet['newName'] = func['name']
        newID = self.getAliasNewId(func['params']['exprId']) 
        alias_deet['newID'] = newID
        self.alias_details.append(alias_deet)
        print(self.alias_details)
        return self.alias_details 


    def getRepartitionDetails(self, other):
        return True

    def findActions(self):
        for other in self.others:
            if other['name'] == 'Join':
                # this is a Join Action
                self.join_details = self.getJoinDetails(other)
            if other['name'] == 'Aggregate':
                # this is an Aggregate Action
                self.agg = 1
                self.aggregate_details = self.getAggregateDetails(other) 
            if other['name'] == 'Repartition':
                # this is a Repartition Action
                self.repartition_details = self.getRepartitionDetails(other)
        for func in self.functions:
            if func['extra']['simpleClassName'] == 'Alias':
                self.alias_details = self.getAliasDetails(func)
        return True

    def getOpNames(self):
        names = self.outputNames
        opNames = []
        for name in names:
            opNames.append(name['newName'])
        return opNames

    def getOpIds(self):
        ids = self.outputId
        opIds = []
        for id in ids:
            opIds.append(id['newID'])
        return opIds

    def getOpDts(self):
        dts = self.outputDataType
        opDts = []
        for dt in dts:
            opDts.append(dt['newDatatype'])
        return opDts

    def getOpDest(self):
        dest = self.outputDest
        opDest = []
        for de in dest:
            opDest.append(de['outputSource'])
        return opDest

    def getSourceCols(self):
        readIds = self.readSplineId
        outIds = self.outputId
        onames = self.outputNames
        noutNames = []
        noutIds = []
        i = 0
        for id in readIds:
            ind = readIds.index(id)
            for oid in outIds:
                if 'originalID' not in oid:
                    noutNames.append('-----')
                    noutIds.append('-----')
                else:    
                    noutNames.append(onames[ind]['newName'])
                    noutIds.append(outIds[i]['newID'])
            i += 1
        return noutNames, noutIds 

    def opAggCreateColOutput(self):
        # original colNames and lineage ids, with source file/table name
        for read in self.reads:
            col_names = []
            spline_id = []
            data_type = []
            col_names = self.readNames
            spline_id = self.readSplineId
            data_type = self.readDataType
            out_colnames = []
            out_splineid = []
            out_datatype = []
            out_dest = []
            z = 0
            ocol_names = self.outputNames
            oIds = self.outputId
            dest = self.outputDest
            for col in self.readNames:
                cind = col_names.index(col)
                z = 0
                for ocol in ocol_names:
                    if col == ocol['newName']:
                        out_colnames.append(ocol['newName'])
                        out_splineid.append(spline_id[cind])
                        out_datatype.append(data_type[cind])
                        out_dest.append(dest[0]['outputSource'])
                        z = 1
                    elif 'originalName' in ocol:
                        xind = ocol_names.index(ocol)
                        if col == ocol['originalName']:
                            out_colnames.append(ocol['newName'])
                            out_splineid.append(oIds[xind]['newID'])
                            out_datatype.append(data_type[cind])
                            out_dest.append(dest[0]['outputSource'])
                            z = 1
                if z == 0:
                    out_colnames.append('--')
                    out_splineid.append('--')
                    out_datatype.append('--')
                    out_dest.append('--')
        return out_colnames, out_splineid, out_datatype, out_dest
        
    def opCreateColsheet(self):
        print('self_agg', self.agg)
        if self.agg == 1:
            out_colnames, out_splineid, out_datatype, out_dest = self. opAggCreateColOutput()
            opNames = out_colnames
            opIds = out_splineid
            opDts = out_datatype
            opDest = out_dest
        else:
            opNames = self.getOpNames()
            opIds = self.getOpIds()
            opDts = self.getOpDts()
            opDest = self.getOpDest()
        data = {'input_col_names': self.readNames, 'Input_spline_ids': self.readSplineId, \
                'Input_Datatype': self.readDataType, 'Input_Source': self.readSource, \
                'Output_names': opNames, 'output_spline_id': opIds, \
                'Output_datatype': opDts, 'Output_dest': opDest}
        df = pd.DataFrame(data)
        df.to_csv('join_columns.csv')
        return True

    def opCreateActsheet(self):
        print('creating action sheet')
        workbook = xlsxwriter.Workbook('join_action.xlsx')
        for other in self.others:
            if other['name'] == 'Join':
                wsht = workbook.add_worksheet('Join')
                wsht.write(0,0, 'Join')
                wsht.write(0, 1, 'Input Sources: ')
                wsht.write(0, 2, self.join_details[0]['inputSources'])
                wsht.write(1, 2, self.join_details[1]['inputSources'])
                wsht.write(2, 1, 'Match columns:')
                wsht.write(2, 2, self.join_details[2]['match_cols'][0])
                wsht.write(3, 2, self.join_details[2]['match_cols'][1])
                wsht.write(4, 1, "Expression Type:")
                wsht.write(4, 2, self.join_details[2]['expr_type'])
                print(self.join_details)
            if other['name'] == 'Aggregate':
                wsht = workbook.add_worksheet('Aggregate')
                wsht.write(0,0, 'Aggregation')
                agg_acts = self.aggregate_details[0]['actions']
                agg_deets = self.aggregate_details
                wsht.write(0, 1, "Data Source:")
                wsht.write(0, 2, agg_deets[0]['source'])
                wsht.write(1, 1, "Group By:")
                wsht.write(1, 2, agg_deets[0]['groupColumn'])
                wsht.write(2, 1, "Aggregate field: ")
                wsht.write(2, 2, agg_deets[0]['AggregateColumns'][0])
                wsht.write(2, 3, agg_deets[0]['AggregateColumns'][1])   
                wsht.write(3, 1, "Actions:" )
                wsht.write(3, 2, agg_acts[0])
                wsht.write(4, 2, agg_acts[1])
                wsht.write(5, 2, agg_acts[2])
                wsht.write(6, 2, agg_acts[3])
            if other['name'] == 'Repartition':
                wsht = workbook.add_worksheet()
                for other in self.others:
                    if other['name'] == 'Repartition':
                        wsht.write(0, 0, 'Repartition')
                        output_file = self.writes['outputSource'].split('/')[-1]
                        wsht.write(0, 1, 'Output file')
                        wsht.write(0, 2, output_file)
                        wsht.write(1, 1, 'Number of partitions')
                        wsht.write(1, 2, other['params']['numPartitions'])
            if other['name'] == 'Project':
                print('alias')
                wsht = workbook.add_worksheet()
                wsht.write(0, 0, 'Alias')
                wsht.write(0, 1, 'Data Type')
                wsht.write(0, 2, self.alias_details[0]['dataType'])
                wsht.write(1, 1, 'Original Name')
                wsht.write(1, 2, self.alias_details[0]['originalName'])
                wsht.write(2, 1, 'Original Id')
                wsht.write(2, 2, self.alias_details[0]['originalID'])
                wsht.write(3, 1, 'New Name')
                wsht.write(3, 2, self.alias_details[0]['newName'])
                wsht.write(4, 1, 'New Id')
                wsht.write(4, 2, self.alias_details[0]['newID'])
                print(self.alias_details)
        workbook.close()


if __name__ == '__main__':
    jlineage = 'lineage_join.json' 
    parse = ParseLineage(jlineage)
    parse.loadLineage()
    parse.getObjects()
    parse.getReads()
    parse.getWrite()
    parse.findActions()
    parse.opCreateColsheet()
    parse.opCreateActsheet()