import { exec } from "child_process"
import { join } from "path"
import { Database, Statement } from 'sqlite3'

type DatabaseMap = { [name: string]: Database }

const databaseBaseDir = join(__dirname, 'databases')
const openDatabases: DatabaseMap = {}
const closedDabases: DatabaseMap = {}


const echoStringValue = ([options]: any) => {
    return new Promise((resolve) => resolve(options[0].value))
}

const openDatabase = ([options]: any) => {
    let databaseName = options[0].name
    
    if(openDatabases[databaseName] != null) {
        console.error(`INTERNAL OPEN ERROR: db already open for: ${databaseName}`)
        return
    }

    if(closedDabases[databaseName] != null) {
        let db = openDatabases[databaseName] = closedDabases[databaseName]
        delete closedDabases[databaseName]
        try {
            db.exec('ROLLBACK')
        } catch(e) {
            return
        }
    }
    openDatabases[databaseName] = new Database(databaseName)
}

const sqlResultFormatter = (t: any,  e: Error, rows: any[]): Promise<any> => {
    return new Promise((resolve, reject) => {
        if(e != null) {
            reject({
                type: 'error',
                result: {
                    code: 0,
                    message: e.toString()
                }
            })
        }
        else {
            let o: any = {
                type: 'success'
            }
            if(t.changes != null && t.changes !== 0) {
                o.result = {
                    rows: rows,
                    insertId: t.lastID,
                    rowsAffected: t.changes
                }
            }
            else {
                o.result = {
                    rows: rows,
                    rowsAffected: 0
                }
            }
            resolve(o)
        }
    })
}

const executeSql = (db: Database, sql: string, params: any) => {
    return new Promise((resolve, reject) => {
        if(sql.substring(0, 11).toUpperCase() === 'INSERT INTO') {
            db.run(sql, params, function(e: Error, rows: any[]) {
                sqlResultFormatter(this, e, rows).then(resolve).catch(resolve)
            })
        }
        else {
            db.all(sql, params, function(e: Error, rows: any[]) {
                sqlResultFormatter(this, e, rows).then(resolve).catch(resolve)
            })
        }
    })
}

const backgroundExecuteSqlBatch = async ([options]: any) => {
    let databaseName = options[0].dbargs.dbname

    if(openDatabases[databaseName] == null)
        throw new Error('INTERNAL ERROR: database is not open')
    
    let db = openDatabases[databaseName]

    let executes = options[0].executes

    let resultList = []

    for(let i = 0; i < executes.length; ++i) {
        let sql = executes[i].sql
        let params = executes[i].params

        resultList.push(await executeSql(db, sql, params))
    }
    return resultList
}

const closeDatabase = ([options]: any) => {
    let databaseName = options[0].path

    let db = openDatabases[databaseName]
    if(db == null)
        throw new Error('INTERNAL CLOSE ERROR: database not open')
    closedDabases[databaseName] = openDatabases[databaseName]
    delete openDatabases[databaseName]
}

const deleteDatabase = ([options]: any) => {
    let databaseName = options[0].path

    if(closedDabases[databaseName] != null) {
        closedDabases[databaseName].close()
        delete closedDabases[databaseName]
    }
    else {
        closeDatabase(options)
        deleteDatabase(options)
    }
}

module.exports = {
    delete: deleteDatabase,
    echoStringValue: echoStringValue,
    open: openDatabase,
    close: closeDatabase,
    backgroundExecuteSqlBatch: backgroundExecuteSqlBatch
}