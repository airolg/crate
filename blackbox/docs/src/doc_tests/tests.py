# -*- coding: utf-8; -*-
#
# Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

import unittest
import doctest
import zc.customdoctests
from crate.testing.layer import CrateLayer
import os
import time
import shutil
import re
import tempfile
import logging
from functools import partial
from . import process_test
from testutils.paths import crate_path, project_path
from testutils.ports import GLOBAL_PORT_POOL
from crate.crash.command import CrateCmd
from crate.crash.printer import PrintWrapper, ColorPrinter
from crate.client import connect


CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()

log = logging.getLogger('crate.testing.layer')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
log.addHandler(ch)


class CrateTestCmd(CrateCmd):

    def __init__(self, **kwargs):
        super(CrateTestCmd, self).__init__(**kwargs)
        doctest_print = PrintWrapper()
        self.logger = ColorPrinter(False, stream=doctest_print, line_end='\n')

    def stmt(self, stmt):
        stmt = stmt.replace('\n', ' ')
        if stmt.startswith('\\'):
            self.process(stmt)
        else:
            self.execute(stmt)


cmd = CrateTestCmd(is_tty=False)


def wait_for_schema_update(schema, table, column):
    conn = connect('localhost:' + str(CRATE_HTTP_PORT))
    c = conn.cursor()
    count = 0
    while count == 0:
        c.execute(
            ('select count(*) from information_schema.columns '
             'where table_schema = ? and table_name = ? '
             'and column_name = ?'),
            (schema, table, column)
        )
        count = c.fetchone()[0]


def wait_for_function(signature):
    conn = connect('localhost:' + str(CRATE_HTTP_PORT))
    c = conn.cursor()
    wait = 0.0

    while True:
        try:
            c.execute('SELECT ' + signature)
        except Exception as e:
            wait += 0.1
            if wait >= 2.0:
                raise e
            else:
                time.sleep(0.1)
        else:
            break


def bash_transform(s):
    # The examples in the docs show the real port '4200' to a reader.
    # Our test suite requires the port to be '44200' to avoid conflicts.
    # Therefore, we need to replace the ports before a test is being run.
    s = s.replace(':4200/', ':{0}/'.format(CRATE_HTTP_PORT))
    if s.startswith("crash"):
        s = re.search(r"crash\s+-c\s+\"(.*?)\"", s).group(1)
        return u'cmd.stmt({0})'.format(repr(s.strip().rstrip(';')))
    return (
        r'import subprocess;'
        r'print(subprocess.check_output(r"""%s""",stderr=subprocess.STDOUT,shell=True).decode("utf-8"))' % s
    ) + '\n'


def crash_transform(s):
    # The examples in the docs show the real port '4200' to a reader.
    # Our test suite requires the port to be '44200' to avoid conflicts.
    # Therefore, we need to replace the ports before a test is being run.
    if s.startswith('_'):
        return s[1:]
    s = s.replace(':4200', ':{0}'.format(CRATE_HTTP_PORT))
    return u'cmd.stmt({0})'.format(repr(s.strip().rstrip(';')))


bash_parser = zc.customdoctests.DocTestParser(
    ps1='sh\$', comment_prefix='#', transform=bash_transform)

crash_parser = zc.customdoctests.DocTestParser(
    ps1='cr>', comment_prefix='#', transform=crash_transform)


class ConnectingCrateLayer(CrateLayer):

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('settings', {})
        self.repo_path = kwargs['settings']['path.repo'] = tempfile.mkdtemp()
        super(ConnectingCrateLayer, self).__init__(*args, **kwargs)

    def start(self):
        super(ConnectingCrateLayer, self).start()
        cmd._connect(self.crate_servers[0])

    def tearDown(self):
        shutil.rmtree(self.repo_path, ignore_errors=True)
        super(ConnectingCrateLayer, self).tearDown()


crate_layer = ConnectingCrateLayer(
    'crate',
    host='localhost',
    crate_home=crate_path(),
    port=CRATE_HTTP_PORT,
    transport_port=CRATE_TRANSPORT_PORT,
    env={'JAVA_HOME': os.environ.get('JAVA_HOME', '')},
    settings={
         # The disk.watermark settings can be removed once crate-python > 0.21.1 has been released
        'cluster.routing.allocation.disk.watermark.low': '100k',
        'cluster.routing.allocation.disk.watermark.high': '10k',
        'cluster.routing.allocation.disk.watermark.flood_stage': '1k',
        'license.enterprise': 'true',
        'lang.js.enabled': 'true'
    }
)


def setUpLocations(test):
    test.globs['cmd'] = cmd
    test.globs['wait_for_function'] = wait_for_function
    cmd.stmt("""
        create table locations (
          id string primary key,
          name string,
          "date" timestamp,
          kind string,
          position integer,
          description string,
          race object(dynamic) as (
            interests array(string),
            description string,
            name string
          ),
          information array(object as (
              population long,
              evolution_level short
            )
          ),
          index name_description_ft using fulltext(name, description) with (analyzer='english')
        ) clustered by(id) into 2 shards with (number_of_replicas=0)""".strip())
    cmd.stmt("delete from locations")
    locations_file = get_abspath("locations.json")
    cmd.stmt("""copy locations from '{0}'""".format(locations_file))
    cmd.stmt("""refresh table locations""")


def tearDownLocations(test):
    cmd.stmt("""drop table if exists locations""")


def setUpUserVisits(test):
    test.globs['cmd'] = cmd
    cmd.stmt("""
        create table uservisits (
          id integer primary key,
          name string,
          visits integer,
          last_visit timestamp
        )
    """.strip())
    uservisits_file = get_abspath("uservisits.json")
    cmd.stmt("""copy uservisits from '{0}'""".format(uservisits_file))
    cmd.stmt("""refresh table uservisits""")


def tearDownUserVisits(test):
    cmd.stmt("""drop table if exists uservisits""")


def setUpArticles(test):
    test.globs['cmd'] = cmd
    cmd.stmt("""
        create table articles (
          id integer primary key,
          name string,
          price float
        ) clustered by(id) into 2 shards with (number_of_replicas=0)""".strip())
    articles_file = get_abspath("articles.json")
    cmd.stmt("""copy articles from '{0}'""".format(articles_file))
    cmd.stmt("""refresh table articles""")


def tearDownArticles(test):
    cmd.stmt("""drop table if exists articles""")


def setUpColors(test):
    test.globs['cmd'] = cmd
    cmd.stmt("""
        create table colors (
          id integer primary key,
          name string,
          rgb string,
          coolness float
        ) with (number_of_replicas=0)""".strip())
    colors_file = get_abspath("colors.json")
    cmd.stmt("""copy colors from '{0}'""".format(colors_file))
    cmd.stmt("""refresh table colors""")


def tearDownColors(test):
    cmd.stmt("""drop table if exists colors""")


def setUpEmployees(test):
    test.globs['cmd'] = cmd
    cmd.stmt("""
        create table employees (
          id integer primary key,
          name string,
          surname string,
          dept_id integer,
          sex string
        ) with (number_of_replicas=0)""".strip())
    emp_file = get_abspath("employees.json")
    cmd.stmt("""copy employees from '{0}'""".format(emp_file))
    cmd.stmt("""refresh table employees""")


def tearDownEmployees(test):
    cmd.stmt("""drop table if exists employees""")


def setUpDepartments(test):
    test.globs['cmd'] = cmd
    cmd.stmt("""
        create table departments (
          id integer primary key,
          name string,
          manager_id integer,
          location integer
        ) with (number_of_replicas=0)""".strip())
    dept_file = get_abspath("departments.json")
    cmd.stmt("""copy departments from '{0}'""".format(dept_file))
    cmd.stmt("""refresh table departments""")


def tearDownDepartments(test):
    cmd.stmt("""drop table if exists departments""")


def setUpQuotes(test):
    test.globs['cmd'] = cmd
    cmd.stmt("""
        create table quotes (
          id integer primary key,
          quote string,
          index quote_ft using fulltext (quote)
        ) clustered by(id) into 2 shards with(number_of_replicas=0)""")

    import_dir = '/tmp/import_data'
    if not os.path.isdir(import_dir):
        os.mkdir(import_dir)
    shutil.copy(
        project_path('sql/src/test/resources/essetup/data/copy',
                     'test_copy_from.json'),
        os.path.join(import_dir, "quotes.json")
    )


def tearDownQuotes(test):
    cmd.stmt("""drop table if exists quotes""")


def setUpPhotos(test):
    test.globs['cmd'] = cmd
    cmd.stmt("""
        create table photos (
          name string,
          location geo_point
        ) with(number_of_replicas=0)""".strip())
    dept_file = get_abspath("photos.json")
    cmd.stmt("""copy photos from '{0}'""".format(dept_file))
    cmd.stmt("""refresh table photos""")


def tearDownPhotos(test):
    cmd.stmt("""drop table if exists photos""")


def setUpCountries(test):
    test.globs['cmd'] = cmd
    cmd.stmt("""
        create table countries (
          name string,
          "geo" geo_shape INDEX using GEOHASH with (precision='1km'),
          population long
        ) with(number_of_replicas=0)""".strip())
    dept_file = get_abspath("countries.json")
    cmd.stmt("""copy countries from '{0}'""".format(dept_file))
    cmd.stmt("""refresh table countries""")


def tearDownCountries(test):
    cmd.stmt("""drop table if exists countries""")


def setUpLocationsAndQuotes(test):
    setUpLocations(test)
    setUpQuotes(test)


def tearDownLocationsAndQuotes(test):
    tearDownLocations(test)
    tearDownQuotes(test)


def setUpColorsAndArticles(test):
    setUpColors(test)
    setUpArticles(test)


def tearDownColorsAndArticles(test):
    tearDownArticles(test)
    tearDownColors(test)


def setUpLocationsQuotesAndUserVisits(test):
    setUpLocationsAndQuotes(test)
    setUpUserVisits(test)


def tearDownLocationsQuotesAndUserVisits(test):
    tearDownLocationsAndQuotes(test)
    tearDownUserVisits(test)


def setUpEmployeesAndDepartments(test):
    setUpEmployees(test)
    setUpDepartments(test)


def tearDownEmployeesAndDepartments(test):
    tearDownEmployees(test)
    tearDownDepartments(test)


def setUpPhotosAndCountries(test):
    setUpPhotos(test)
    setUpCountries(test)


def tearDownPhotosAndCountries(test):
    tearDownPhotos(test)
    tearDownCountries(test)


def setUpEmpDeptAndColourArticlesAndGeo(test):
    setUpEmployeesAndDepartments(test)
    setUpColorsAndArticles(test)
    setUpPhotosAndCountries(test)


def tearDownEmpDeptAndColourArticlesAndGeo(test):
    tearDownEmployeesAndDepartments(test)
    tearDownColorsAndArticles(test)
    tearDownPhotosAndCountries(test)


def setUpTutorials(test):
    setUp(test)
    import_dir = '/tmp/best_practice_data'
    source_dir = 'sql/src/test/resources/essetup/data/best_practice'
    if not os.path.isdir(import_dir):
        os.mkdir(import_dir)
    shutil.copy(project_path(source_dir, 'data_import.json'),
                os.path.join(import_dir, "users.json"))
    shutil.copy(project_path(source_dir, 'data_import.json.gz'),
                os.path.join(import_dir, "users.json.gz"))
    shutil.copy(project_path(source_dir, 'data_import_1408312800.json'),
                os.path.join(import_dir, "users_1408312800.json"))


def setUp(test):
    test.globs['cmd'] = cmd
    test.globs['wait_for_schema_update'] = wait_for_schema_update


docsuite = partial(doctest.DocFileSuite,
                   optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
                   encoding='utf-8')


def test_suite():
    suite = unittest.TestSuite()

    # Graceful stop tests
    process_suite = unittest.TestLoader().loadTestsFromModule(process_test)
    suite.addTest(process_suite)

    # Documentation tests
    docs_suite = unittest.TestSuite()
    s = docsuite('../../general/blobs.txt', parser=bash_parser, setUp=setUp)
    s.layer = crate_layer
    docs_suite.addTest(s)
    for fn in ('interfaces/http.txt',):
        s = docsuite('../../' + fn,
                     parser=bash_parser,
                     setUp=setUpLocations,
                     tearDown=tearDownLocations)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/ddl/create-table.txt',
               'general/ddl/generated-columns.txt',
               'general/ddl/constraints.txt',
               'general/ddl/sharding.txt',
               'general/ddl/replication.txt',
               'general/ddl/column-policy.txt',
               'general/ddl/fulltext-indices.txt',
               'general/ddl/system-columns.txt',
               'general/ddl/alter-table.txt',
               'general/ddl/storage.txt',
               'admin/runtime-config.txt',
               'general/ddl/show-create-table.txt',
               'general/user-defined-functions.txt',
               'admin/user-management.txt',
               'admin/privileges.txt',
               'admin/ingestion/rules.txt',
               'general/dql/index.txt',
               'general/dql/refresh.txt',
               'admin/optimization.txt',
               'general/dql/fulltext.txt',
               'general/ddl/data-types.txt',
               'general/occ.txt',
               'general/information-schema.txt',
               'general/ddl/partitioned-tables.txt',
               'general/builtins/aggregation.txt',
               'general/builtins/arithmetic.txt',
               'general/builtins/scalar.txt',
               'general/builtins/table-functions.txt',
               'admin/system-information.txt',
               'general/dql/selects.txt',
               'interfaces/postgres.txt'):
        s = docsuite('../../' + fn, parser=crash_parser,
                     setUp=setUpLocationsAndQuotes,
                     tearDown=tearDownLocationsAndQuotes)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/dql/geo.txt',):
        s = docsuite('../../' + fn,
                     parser=crash_parser,
                     setUp=setUpCountries,
                     tearDown=tearDownCountries)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/dql/joins.txt',
               'general/builtins/subquery-expressions.txt',):
        path = os.path.join('..', '..', fn)
        s = docsuite(path,
                     parser=crash_parser,
                     setUp=setUpEmpDeptAndColourArticlesAndGeo,
                     tearDown=tearDownEmpDeptAndColourArticlesAndGeo)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/dml.txt',):
        s = docsuite('../../' + fn,
                     parser=crash_parser,
                     setUp=setUpLocationsQuotesAndUserVisits,
                     tearDown=tearDownLocationsQuotesAndUserVisits)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('admin/snapshots.txt',):
        s = docsuite('../../' + fn,
                     parser=crash_parser,
                     setUp=setUpLocationsAndQuotes,
                     tearDown=tearDownLocationsAndQuotes)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/dql/union.txt',):
        path = os.path.join('..', '..', fn)
        s = doctest.DocFileSuite(path,
                                 parser=crash_parser,
                                 setUp=setUpPhotosAndCountries,
                                 tearDown=tearDownPhotosAndCountries,
                                 optionflags=doctest.NORMALIZE_WHITESPACE |
                                             doctest.ELLIPSIS,
                                 encoding='utf-8')
        s.layer = crate_layer
        docs_suite.addTest(s)

    suite.addTests(docs_suite)
    return suite


def get_abspath(name):
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), name)
    )
