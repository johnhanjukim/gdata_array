#!/usr/bin/env python
import argparse
import collections
import ConfigParser
import gdata.auth
import gdata.spreadsheet
import gdata.spreadsheet.service
import logging
import os
import re
import time
import xml.sax.saxutils

"""
Module for interacting with Google Docs spreadsheets.  This has an 
intuitive object-oriented interface where each worksheet acts like 
a two-dimensional array. 

ws = google.worksheet(gdata_key, title='worksheet 1')
for row in ws:
    for cell in row:
        print cell

The worksheet and row objects act as lists, while the cell acts as a string. 

It currently only allows access as a single user at a time.  The user 
data, including password should be put into the configuration file in 
standard config file format (section 'GOOGLE').  

cf. An alternate solution, https://github.com/burnash/gspread/
"""

# The following are public module variables
config_filename = '.config'
config_section = 'GOOGLE'
email = ''
password = ''
source = 'gdata_array-v1'
num_tries = 5
retry_wait_time_seconds = 2
# Identify key within a Google Doc spreadsheet public URL, like 
# https://docs.google.com/a/spreadsheet/ccc?key=(<key>)&pli=1#gid=0
gdata_key_pattern = re.compile('https?://.*key=(\w+)', re.I)
# Identify ID within a feed URL returned by the gdata API, like
#https://spreadsheets.google.com/feeds/worksheets/<key>/private/full/<wksht_id>
gdata_id_pattern = re.compile('https?://.*/(\w+)', re.I)

# Sequence of column tags for blank
blank_coltags = ('_cn6ca', '_cokwr', '_cpzh4', '_cre1l', '_chk2m', 
                 '_ciyn3', '_ckd7g', '_clrrx', '_cyevm', '_cztg3', 
                 '_d180g', '_d2mkx', '_cssly', '_cu76f', '_cvlqs', 
                 '_cx0b9', '_d9ney', '_db1zf', '_dcgjs', '_ddv49', 
                 '_d415a', '_d5fpr', '_d6ua4', '_d88ul', '_dkvya', 
                 '_dmair', '_dnp34', '_dp3nl', '_df9om')

# The following are private module variables
_spreadsheet_service = None

def read_config_file():
    global email, password, source
    fullpath = os.path.dirname(__file__) + os.sep + config_filename
    parser = ConfigParser.ConfigParser()
    try:
        parser.read( fullpath )
        email = parser.get(config_section, 'email', email)
        password = parser.get(config_section, 'password', password)
        source = parser.get(config_section, 'source', source)
    except Exception, e:
        logging.warn("Error reading config file %s" % fullpath)
        raise e

def spreadsheet_service():
    global _spreadsheet_service
    if ( (not _spreadsheet_service) or 
         (_spreadsheet_service.email != email) or
         (_spreadsheet_service.password != password) or
         (_spreadsheet_service.source != source) ):
        read_config_file()
        _spreadsheet_service = gdata.spreadsheet.service.SpreadsheetsService()
        _spreadsheet_service.email = email
        _spreadsheet_service.password = password
        _spreadsheet_service.source = source
        _spreadsheet_service.ProgrammaticLogin()
    return _spreadsheet_service

def GetWorksheetsFeed(*args, **kwargs):
    logging.info('GetWorksheetsFeed(%s,%s)', args, kwargs)
    return spreadsheet_service().GetWorksheetsFeed(*args, **kwargs)

def AddWorksheet(*args, **kwargs):
    logging.info('AddWorksheet(%s, %s)', args, kwargs)
    return spreadsheet_service().AddWorksheet(*args, **kwargs)

def GetCellsFeed(*args, **kwargs):
    logging.info('GetCellsFeed(%s, %s)', args, kwargs)
    return spreadsheet_service().GetCellsFeed(*args, **kwargs)

def GetListFeed(*args, **kwargs):
    logging.info('GetListFeed(%s, %s)', args, kwargs)
    return spreadsheet_service().GetListFeed(*args, **kwargs)

def UpdateCell(*args, **kwargs):
    logging.info('UpdateCell(%s, %s)', args, kwargs)
    for i in range(1,num_tries+1):
        try:
            return spreadsheet_service().UpdateCell(*args, **kwargs)
        except Exception, e:
            if (i < num_tries):
                logging.warn(e)
                logging.warn('Retrying UpdateCell')
                time.sleep(retry_wait_time_seconds)
            else:
                raise e

def InsertRow(*args, **kwargs):
    logging.info('InsertRow(%s, %s)', args, kwargs)
    return spreadsheet_service().InsertRow(*args, **kwargs)

def spreadsheet(key):
    """
    Returns a Spreadsheet object that acts as a list of worksheet objects.
    """
    ss = Spreadsheet()
    ss.extend( worksheets(key) )
    return ss

def worksheets(key, titles=None):
    """
    This returns a simplified list-like object that includes 
    simple key, wksht_id, and title fields - and accesses rows 
    as list items. 

    The optional key "titles" returns only those worksheets with 
    titles in that set.  
    """
    # Strip off a trailing "#gid=0" at the end of an ID
    # This can come from cut-and-paste from a GDoc URL
    key = re.sub(r'\#gid=\d+$', '', key)
    feed = GetWorksheetsFeed(key)
    wslist = []
    for wsdata in feed.entry:
        ws = Worksheet(key, wsdata, feed)
        if ((titles==None) or (ws.title in titles)): wslist.append(ws)
    return wslist

def worksheet(key, num=None, wksht_id=None, title=None, nheaders=None):
    """
    Return a single worksheet object from the spreadsheet at key, 
    specifying wksht_id or title or both. 
    Raises an exception if there are multiple or zero matches found.
    """
    if ((num != None) and (num <= 0)): 
        raise ValueError('Invalid num argument (should be 1,2,...): %s' % num)

    wslist = worksheets(key)
    if (wksht_id==None and title==None and num==None):
        if (len(wslist) == 1):
            if (nheaders != None): 
                wslist[0].nheaders = nheaders
            return wslist[0]
        else:
            raise ValueError("Must specify wksht_id or title")
    else:
        filtered = []
        titles = []
        for i,ws in enumerate(wslist):
            titles.append(ws.title)
            if (wksht_id and ws.wksht_id != wksht_id): continue
            if (title and ws.title != title): continue
            if (num and num != (i+1)): continue
            filtered.append(ws)
        if (len(filtered) == 1):
            if (nheaders != None): 
                filtered[0].nheaders = nheaders
            return filtered[0]
        else:
            raise ValueError('%d worksheets matching num=%s wksht_id=%s title="%s"\ntitles=%s' % (len(filtered), num, wksht_id, title, titles))

def add_worksheet(key, title, rows=10, cols=10, nheaders=1):
    """
    Adds a worksheet to the specified spreadsheet, and returns an 
    worksheet array object.  
    """
    wsdata = AddWorksheet(title, rows, cols, key)
    ws = Worksheet(key, wsdata, nheaders=nheaders)
    return ws

def wksht_ids(key, titles=None):
    """
    Returns the list of short wksht_id strings in a given spreadsheet.
    """
    # Strip off a trailing "#gid=0" at the end of an ID
    # This can come from cut-and-paste from a GDoc URL
    key = re.sub(r'\#gid=\d+$', '', key)
    feed = GetWorksheetsFeed(key)
    ids = []
    for ws in feed.entry:
        if ((titles != None) and (ws.title.text not in titles)):
            continue
        ids.append( WorksheetID(full_id=ws.id.text) )
    return ids

def wksht_id(key, title=None):
    """
    This as input one of: 
    (1) a full URL-form worksheet ID which it parses to find wksht_id, 
    or 
    (2) a GDoc key for a spreadsheet that it accesses to find the wksht_id. 

    If #2, it can take an optional argument of a worksheet title to match.
    Otherwise it will only return if there is a single worksheet in the 
    spreadsheet. 

    It returns a single short-style wksht_id object (subclass of string). 
    """
    if (gdata_id_pattern.match(key)):
        return WorksheetID(full_id=key)
    else:
        ids = wksht_ids(key, [title])
        if (len(ids) == 1):
            return ids[0]
        elif (ids):
            raise ValueError('Multiple matching wksht_ids found')
        else:
            raise ValueError('No matching wksht_id found')

def create_worksheet(key, array):
    """
    This takes a large array and creates a new worksheet in one pass 
    by using the CSV import feature.  
    """
    svc.SetOAuthInputParameters(gdata.auth.OAuthSignatureMethod.HMAC_SHA1, 'anonymous', 'anonymous')
    token = svc.FetchOAuthRequestToken()
    raise NotImplementedError()

    # POST /a/spreadsheet/info/tc?id=tmvEpRQK3raevlL3yqqj0QQ.15810189397340970591.3707098243111392783&tfe=ih_598&gsessionid=XH1O2GlKpaA HTTP/1.1
    # returns plain text FpdJbD4BAAA.WqfYrYosmUVrBck0flAEdA.dwAu6ipjOvhzd5LbONd4wA
    #
    # Below is a sample captured HTTP for creating a new worksheet:
    # The spreadsheet has the following key:
    # 0AqcMI7sDzBmudG12RXBSUUszcmFldmxMM3lxcWowUVE#gid=2
    #
    # POST /a/spreadsheet/convert/import?id=tmvEpRQK3raevlL3yqqj0QQ.15810189397340970591.3707098243111392783&tfe=ih_598&gsessionid=XH1O2GlKpaA&c=0&r=0&agi=0&dst=NEW_SHEET&del&token=FpdJbD4BAAA.WqfYrYosmUVrBck0flAEdA.dwAu6ipjOvhzd5LbONd4wA HTTP/1.1
    # POST Data
    # Content-Disposition: form-data; name="file"; filename="sample_data.csv"
    # Content-Type: text/csv

    # token.oauth_input_params = client._oauth_input_params 
    #         client.SetOAuthInputParameters(
    #gdata.auth.OAuthSignatureMethod.HMAC_SHA1,
    #        GDATA_CREDS['key'],
    #        consumer_secret=GDATA_CREDS['secret'],
    #    )
    #    client.SetOAuthTok

######################################################################
class WorksheetID(str):
    """
    This is a class for the short-form ID for a Google Doc worksheet.
    It takes either a long URL-form ID or a short form.

    TODO: add in more validation
    """
    def __new__(cls, full_id=None, short_id=None):
        if (full_id and not short_id):
            gdata_id_match = gdata_id_pattern.match(full_id)
            if (gdata_id_match):
                short_id = gdata_id_match.group(1)
            else:
                raise ValueError('Could not parse full worksheet ID "%s"' % full_id)
        elif (full_id and short_id):
            raise ValueError('Cannot specify both full and short worksheet IDs')
        obj = super(WorksheetID, cls).__new__(cls, short_id)
        obj.full_id = full_id
        obj.short_id = short_id
        return obj

######################################################################
class Spreadsheet(list):
    """
    This acts as a list of Worksheet objects. 
    """
    def __init__(self, gdata_ws_feed):
        self.title = None
        raise NotImplementedError()

######################################################################
class Worksheet(object):
    """
    This acts like a list of Row objects.  It loads data through the 
    Google gdata API only when that data is called on, rather than 
    upon object creation.  

    for row in worksheet:
        # do whatever with row
    """
    def __init__(self, key, gdata_ws, gdata_ws_feed=None, nheaders=1):
        self.key = key
        self.data = gdata_ws
        self._ws_feed = gdata_ws_feed
        self.nheaders = nheaders

        # If row_count is less than nheaders, error out.
        if (int(self.data.row_count.text) < nheaders):
            raise ValueError("Fewer rows than specified headers")

        # A gdata_ws object is required
        self.wksht_id = wksht_id( gdata_ws.id.text )
        self.title = gdata_ws.title.text
        # The internal data representation
        self._headers = None
        self._rows = None
        self._list_feed = None
        self._cells_feed = None
        self._max_col = 0

    def get_ws_feed(self):
        if (not self._ws_feed):
            self._ws_feed = GetWorksheetsFeed(self.key)
        return self._ws_feed
    ws_feed = property(get_ws_feed, None)

    def get_sstitle(self):
        return self.ws_feed.title.text
    sstitle = property(get_sstitle, None)

    def load_data(self):
        self.get_cells_feed()

    def has_data(self): 
        return (self._cells_feed != None)

    def reload(self): 
        self._cells_feed = None
        self._list_feed = None
        self.get_cells_feed()
        
    def get_cells_feed(self):
        """
        This reads the worksheet data into local memory using the cells 
        feed.  The List feed is potentially more intuitive, but it stops 
        reading after a single blank row, so should be avoided.  
        """
        if (not self._cells_feed):
            logging.info('Creating feed for worksheet "%s"' % self.title)
            self._cells_feed = GetCellsFeed(self.key, self.wksht_id.short_id)
            logging.info('Found %d entries' % len(self._cells_feed.entry))
            self._rows = []
            if (self.nheaders > 1): 
                logging.warn("Only looking at last of multiple header rows")

            self._header_rows = []
            for cell in self._cells_feed.entry:
                row = int(cell.cell.row)
                col = int(cell.cell.col)
                logging.debug('Adding cell for row %d, col %d' % (row, col))
                # Allow for multiple header rows, possibly 
                # including blank rows in them.  
                if (row <= self.nheaders):
                    for i in range(len(self._header_rows), row):
                        self._header_rows.append( Row(self, i+1) )
                    header_row = self._header_rows[row-1]
                    new_cell = Cell(self, cell, row=row, col=col)
                    header_row._set_local(col-1, new_cell)
                else:
                    self.init_cell(row, col, cell)
            # If header rows are blank, ensure they are entered anyway.
            for i in range(len(self._header_rows), self.nheaders):
                logging.debug("Adding header row")
                self._header_rows.append( Row(self, i+1) )
                
        return self._cells_feed

    def get_list_feed(self):
        """
        WARNING: The gdata List Feed is fatally flawed in that it stops
        returning results after a single blank line.  To retrieve a 
        correct list feed, the module temporarily puts a single space 
        character into the first cell of blank rows, gets the list feed, 
        then removes that whitespace. 
        """
        if (not self._list_feed):
            if (not self.has_data()): self.load_data()
            # For the list feed to read correctly, there must be no 
            # blank rows.  
            blank_rows = []
            for row in self.headers+self.rows:
                if (not row): 
                    row[0] = ' '
                    blank_rows.append(row)
            self._list_feed = GetListFeed(self.key, self.wksht_id)
            for row in blank_rows:
                row[0] = None
        return self._list_feed
    list_feed = property(get_list_feed, None)

    def get_coltags(self):
        """
        The gdata spreadsheet API creates column keys according to 
        an undocumented algorithm.  Take the first row; convert 
        each entry text to lowercase; remove all characters except 
        a-z, 0-9, and hyphen; append '_2', '_3', ... for repeated keys.

        The predefined method 
        gdata.spreadsheet.text_db.ConvertStringsToColumnHeaders
        is written to convert column names, but it is *WRONG*.  
        """
        if (not self.has_data()): self.load_data()
        coltags = []
        count = collections.defaultdict(int)
        tag_row = self.get_row(1)
        for i in range(0, max(self.max_col,len(blank_coltags))):
            key = None
            if (i < len(tag_row) and tag_row[i]):
                key = re.sub(r'[^a-zA-Z0-9\-]', '', tag_row[i])
                if (key):
                    count[key] += 1
                    if (count[key] > 1): 
                        key += '_%d' % count[key]
            if (not key):
                key = blank_coltags[i]
            coltags.append(key)
        return coltags
    coltags = property(get_coltags, None)

    def coltag_test(self):
        custom_tags = []
        if (self.list_feed.entry):
            coltags = self.coltags
            missing = []
            for e in self.list_feed.entry:
                for key in e.custom.keys():
                    if (key not in custom_tags):
                        custom_tags.append(key)
                    if (key not in coltags+missing):
                        missing.append(key)
            if (missing):
                logging.warn("unrecognized coltags %s" % missing)
        return custom_tags

    def get_rows(self):
        if (not self.has_data()): self.load_data()
        return self._rows
    rows = property(get_rows, None)

    def get_headers(self):
        """
        This returns a single header row.  If there are multiple header 
        rows, it returns only the last one.  
        """
        if (not self.has_data()): self.load_data()
        if (self._header_rows): return self._header_rows[-1]
        else: return None
    def set_headers(self, vals):
        """
        This sets the header values in the worksheet.
        """
        self.set_row(self.headers.row, vals)
        
    headers = property(get_headers, set_headers)

    def get_all_header_rows(self):
        """
        Returns all header rows as a two-dimensional array. 
        """
        if (not self.has_data()): self.load_data()
        return self._header_rows

    def init_cell(self, row_num, col_num, cell):
        irow = row_num - self.nheaders - 1
        icol = col_num - 1
        logging.debug('Adding cell at irow %d, icol %d' % (irow, icol))
        for blank_row_num in range(self.max_row+1 , row_num+1):
            logging.debug('Adding blank row to worksheet')
            self._rows.append( Row(self, blank_row_num) )
        row = self._rows[irow]
        new_cell = Cell(self, cell, row=row_num, col=col_num)
        if (col_num > self._max_col): self._max_col = col_num
        # Force normal list behavior for the Row class using super(...).
        # Otherwise setitem will try to update the worksheet.
        row._set_local(icol, new_cell)
        
    def get_max_row(self):
        return len(self.rows) + self.nheaders
    max_row = property(get_max_row, None)

    def get_max_col(self):
        if (not self.has_data()): self.load_data()
        return self._max_col
    max_col = property(get_max_col, None)

    def get_row(self, row_num):
        """
        Get row by the spreadsheet row number, starting with 1 for the 
        first row - which might be a header row.
        """
        if (row_num <= self.nheaders):
            return self.get_all_header_rows()[row_num-1]
        else:
            return self.rows[row_num-self.nheaders-1]

    def set_row(self, row_num, vals=[]):
        """
        Set row by the spreadsheet row number, starting with 1 for the 
        first row - which might be a header row.
        """
        row_data = RowData(self, vals)
        row = self.get_row(row_num)
        for i,val in enumerate(row_data):
            if (row[i] != val): row[i] = val

    def __setitem__(self, irow, vals):
        self.set_row(self[irow].row, vals)

    def __len__(self):
        return self.rows.__len__()

    def __getitem__(self, key):
        return self.rows.__getitem__(key)

    def __iter__(self):
        return self.rows.__iter__()

    def __contains__(self, item):
        return self.rows.__contains__(item)

    def append(self, vals=[], overwrite=True):
        """
        Appends the specified data to the worksheet.  Note that if there 
        are blank lines at the end of the worksheet, this will write onto 
        those.  
        """
        row_data = RowData(self, vals)

        # Frustratingly, for InsertRow, gdata does not allow inserting 
        # an empty data. It only accepts column *name* to specify the 
        # column, but also changes the column name by an undocumented 
        # process - cf. Worksheet.get_coltags()
        # 
        # The process is collected in the RowData and RowDataVal classes.

        # Standard append adds to content, not data.  If header rows 
        # are blank, they should be temporarily filled in.  
        blank_rows = []
        for row in self.get_all_header_rows():
            if (not row): 
                row[0] = ' '
                blank_rows.append(row)

        # Insert in values via gdata API
        res = InsertRow(row_data.insert_vals, 
                        key=self.key, wksht_id=self.wksht_id)

        for row in blank_rows: row[0] = None

        # Create a blank row in internal representation
        new_row_num = self.max_row+1
        self.rows.append( Row(self, new_row_num) )

        # Force creation of the new values, because InsertRow is 
        # unreliable.  Example: new row has more columns than 
        # previously used, then silently the new values don't show up. 
        for val in row_data:
            if (overwrite and val and (val.coltag not in res.custom)):
                self[-1][val.icol] = val.val
            else:
                self[-1]._set_local(val.icol, val.val)
        # Remove the added space for a blank row using UpdateCell.
        if (row_data.is_blank()): self[-1][0] = None

    def write_xml(self, f):
        f.write("<worksheet>\n")
        for row in self:
            f.write("<row>\n")
            for i,cell in enumerate(row):
                tagname = re.sub(r'\W', '', self.headers[i])
                if (cell and cell.text):
                    val = xml.sax.saxutils.escape(cell.text)
                else:
                    val = ''
                f.write("<%s>%s</%s>\n" % (tagname, val, tagname))
            f.write("</row>\n\n")
        f.write("</worksheet>\n")

    def get_fullname(self):
        return '"%s" in spreadsheet "%s"' % (self.title, self.sstitle)

    def __repr__(self):
        return '<gdata wksht "%s">' % self.title

######################################################################
class RowData(list):
    """
    This represents input data to the InsertRow function.  It is 
    complex because it requires output both as a list and as a hash 
    with coltags as keys.  
    """
    def __init__(self, worksheet, data):
        self.worksheet = worksheet
        if (hasattr(data, 'keys')):
            # Input is a map: convert to list based on header names.
            self.extend( [None] * len(worksheet.headers) )
            for key,val in data.items():
                if (key not in worksheet.headers): 
                    raise ValueError('Invalid column key "%s"' % key)
                elif (val not in [None, '']):
                    i = worksheet.headers.index(key)
                    self[i] = RowDataVal(i, val)
        else:
            self.extend( [RowDataVal(i,val) for i,val in enumerate(data)] )
        # Now assign the coltags
        for i,tag in enumerate(worksheet.coltags):
            if ((i < len(self)) and self[i]):
                self[i].coltag = tag

    def get_insert_vals(self):
        vals = {}
        for item in self: 
            if (item.val and item.coltag): 
                vals[item.coltag] = item.val
        # We are forced to insert a space character instead of 
        # blank because InsertRow does not accept empty data.
        # In these cases, make sure that the correct blank 
        # is over-written later.
        if (not vals and self.worksheet.coltags):
            insert_vals[self.worksheet.coltags[0]] = ' '
        return vals
    insert_vals = property(get_insert_vals, None)

    def is_blank(self):
        return [x for x in self if x.val] == []
        
class RowDataVal(str):
    def __new__(cls, icol, val):
        if (val == None): val = ''
        else: val = '%s' % val
        obj = super(RowDataVal, cls).__new__(cls, val)
        obj.icol = icol
        obj.val = val
        obj.coltag = None
        return obj


######################################################################
class Row(list):
    def __init__(self, worksheet, row=None):
        self.worksheet = worksheet
        self.row = row

    def get_headers(self):
        return self.worksheet.headers
    headers = property(get_headers, None)

    def delete(self):
        # This uses unhelpful gdata ListFeed to delete the row.
        # gdata ListFeed always assumes one header row, and doesn't 
        # allow access to column numbers.
        sslist = self.worksheet.list_feed.entry[self.row - 2]
        # Check that the internal representation data is the same as gdata
        irow = self.row - self.worksheet.nheaders - 1
        obj_vals = set( self.worksheet._rows[irow] + [None] )
        gdata_vals = set( [None] )
        for val in sslist.custom.values():
            if (val.text): gdata_vals.add(val.text.encode('utf-8'))
        if (obj_vals != gdata_vals):
            raise ValueError("Mismatch of gdata and internal values!!\nobj %s\ngdata %s" % (obj_vals, gdata_vals))
        # Delete the row from Google Docs
        spreadsheet_service().DeleteRow(sslist)
        # Delete the row from the Google Docs list feed
        del self.worksheet.list_feed.entry[self.row - 2]
        # Pop the correct Row obj from the internal representation
        x = self.worksheet._rows.pop(irow)
        logging.info("Deleting row %s" % x)
        logging.debug("Decrementing row number for following rows")
        for i in range(self.row-2, len(self.worksheet)):
            if (len(self.worksheet) > i):
                self.worksheet[i].row -= 1
                for cell in self.worksheet[i]:
                    if (cell): cell.row -= 1

    def get_index_of_key(self, key):
        if (key in self.headers): 
            return self.headers.index(key)
        else: 
            try:
                return int(key)
            except Exception, e:
                raise KeyError('Key "%s" not found in %s' % (key, self))

    def __getitem__(self, key):
        ind = self.get_index_of_key(key)
        if (len(self) > ind):
            return super(Row, self).__getitem__(ind)
        else:
            return None

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError, e:
            return default

    def _set_local(self, icol, new_val):
        while (len(self) <= icol): 
            super(Row, self).append(None)
        super(Row, self).__setitem__(icol, new_val)

    def __setitem__(self, key, new_val):
        """
        This first sets a new value for the row in the GDocs worksheet, 
        and then (if there is no error) in the internal representation. 
        """
        if (new_val != None): 
            new_val = "%s" % new_val
        else: 
            new_val = ''
        icol = self.get_index_of_key(key)
        row = self.row
        col = icol + 1
        if (new_val == self[icol]):
            logging.info('No change to cell value "%s"' % self[icol])
            return
        ws = self.worksheet
        gdata_cell = UpdateCell(row, col, new_val, ws.key, ws.wksht_id)
        if (new_val):
            new_cell = Cell(self, gdata_cell, row=row, col=col)
        else:
            new_cell = None
        while (len(self) < col):
            self.append(None)
        # Use super to force the normal list behavior for setitem
        self._set_local(icol, new_cell)
        # To fit pattern, remove trailing None items in list
        while (self and self[-1] == None): self.pop(-1)
        # Check if we need to revise max_col
        if (len(self) > self.worksheet.max_col):
            self.worksheet._max_col = len(self)

    def get_display(self):
        return '<Row %s of %s>' % (row.row, row.worksheet.fullname)
    display = property(get_display, None)

class Cell(str):
    def __new__(cls, worksheet, cell, row=None, col=None):
        if ((row==None) or (col==None)):
            raise ValueError("Must specify row and column explicitly")
        text = cell.cell.text
        if (text == None): text = ''
        obj = super(Cell, cls).__new__(cls, text)
        obj.worksheet = worksheet
        obj.text = cell.cell.text
        obj.data = cell
        obj.row = int(row)
        obj.col = int(col)
        return obj

    def get_colname(self):
        if (len(self.worksheet.headers) >= self.col):
            return self.worksheet.headers[self.col-1]
        else:
            return None

    def undo_allcaps(self):
        result = ''
        for sub in re.split(r'([A-Z][A-Z]+)', self):
            if (re.match(r'^[A-Z]+$', sub)):
                result += sub[0] + sub[1:].lower()
            else:
                result += sub
        if (result != self):
            logging.info('Undid allcaps in "%s" to "%s"' % (self, result))
        return result

    def is_link(self):
        return (self.href != None)

    def get_href(self):
        val = self.data.cell.inputValue
        match = re.search(r'HYPERLINK\(([\"\'])(.*?)\1.*\)', val, re.I)
        if (match):
            return match.group(2)
        else:
            return None
    href = property(get_href, None)

######################################################################
