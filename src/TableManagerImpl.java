import com.apple.foundationdb.*;
import com.apple.foundationdb.async.*;
import com.apple.foundationdb.directory.*;
import com.apple.foundationdb.tuple.*;

import java.util.HashMap;
import java.nio.file.Path;
import java.security.KeyStore.Entry.Attribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{
  private Database db = null;
  private DirectorySubspace tableMngr = null;

  public TableManagerImpl() {
    FDB fdb = FDB.selectAPIVersion(710);

    try {
        db = fdb.open();
    } catch (Exception e) {
        System.out.println("ERROR: the database was not successfully opened: " + e);
    }

    try {
      tableMngr = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from("Manager")).join();
    } catch (Exception e) {
      System.out.println("ERROR: the metadata directory was not successfully opened: " + e);
    }
  }

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType, String[] primaryKeyAttributeNames) {
    if (attributeNames == null || attributeType == null) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    } else if (primaryKeyAttributeNames == null || primaryKeyAttributeNames.length == 0) {
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    } else {
      List temp = Arrays.asList(attributeNames);
      for (String k : primaryKeyAttributeNames) {
        if (!temp.contains(k))
          return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
      }
    }

    DirectorySubspace table = null;
    try {
      table = tableMngr.create(db, PathUtil.from(tableName)).join();
    } catch (Exception e) {
      return StatusCode.TABLE_ALREADY_EXISTS;
    }
    
    List<String> primaryKeys = Arrays.asList(primaryKeyAttributeNames);
    for (int i = 0; i < attributeNames.length; i++) {
      Transaction tx = db.createTransaction();
      String attrName = attributeNames[i];

      Tuple key = Tuple.from(attrName);
      Tuple value = Tuple.from(attributeType[i].name(), primaryKeys.contains(attrName));

      tx.set(table.pack(key), value.pack());
      tx.commit().join();
    }

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    try {
      tableMngr.remove(db, PathUtil.from(tableName)).join();
    } catch (Exception e) {
      return StatusCode.TABLE_NOT_FOUND;
    }
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    HashMap<String, TableMetadata> tables = new HashMap<String, TableMetadata>();
    
    List<String> tableNames = null;

    try {
      tableNames = tableMngr.list(db).join();
    } catch (Exception e) {
      return null;
    }

    for (String name : tableNames) {
      Transaction tx = db.createTransaction();
      final DirectorySubspace table = tableMngr.open(db, PathUtil.from(name)).join();
      AsyncIterable<KeyValue> kvPairs = tx.getRange(table.range());

      List<String> attributeNames = new ArrayList<String>();
      List<String> primaryKeys = new ArrayList<String>();
      List<AttributeType> attributeTypes = new ArrayList<AttributeType>();
      for (KeyValue kv : kvPairs) {
        String attrName = Tuple.fromBytes(kv.getKey()).getString(1);
        Tuple attrProps = Tuple.fromBytes(kv.getValue());
        attributeNames.add(attrName);
        attributeTypes.add(AttributeType.valueOf(attrProps.getString(0)));
        if (attrProps.getBoolean(1)) {
          primaryKeys.add(attrName);
        }
      }
      tables.put(name, new TableMetadata(attributeNames.toArray(new String[0]), attributeTypes.toArray(new AttributeType[0]), primaryKeys.toArray(new String[0])));
      tx.commit().join();
    }

    return tables;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    DirectorySubspace table = null;
    try {
      table = tableMngr.open(db, PathUtil.from(tableName)).join();
    } catch (Exception e) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    Transaction tx = db.createTransaction();

    if (tx.get(table.pack(Tuple.from(attributeName))).join() != null) {
      tx.cancel();
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }

    Tuple key = Tuple.from(attributeName);
    Tuple value = Tuple.from(attributeType.name(),false);
    tx.set(table.pack(key), value.pack());
    tx.commit().join();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    DirectorySubspace table = null;
    try {
      table = tableMngr.open(db, PathUtil.from(tableName)).join();
    } catch (Exception e) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    Transaction tx = db.createTransaction();

    try {
      tx.clear(table.pack(Tuple.from(attributeName)));
      tx.commit().join();
      return StatusCode.SUCCESS;
    } catch (Exception e) {
      tx.cancel();
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }
  }

  @Override
  public StatusCode dropAllTables() {
    Transaction tx = db.createTransaction();
    removeSubspaces(tx, tableMngr);
    tx.commit().join();
    return StatusCode.SUCCESS;
  }

  private void removeSubspaces(Transaction tx, DirectorySubspace subspace) {
    // Get all the child subspaces
    List<DirectorySubspace> children = new ArrayList<DirectorySubspace>();

    try {
      List<String> childrenNames = subspace.list(db).join();
      childrenNames.forEach(name -> { 
        subspace.remove(tx, PathUtil.from(name)).join();
        children.add(subspace.open(db, PathUtil.from(name)).join());
      });
    } catch (Exception e) {
      tx.clear(subspace.pack());
      return;
    }
    
    // Recursively remove child subspaces
    for (DirectorySubspace child : children) {
      removeSubspaces(tx, child);
    }
    // Remove this subspace
    tx.clear(subspace.pack());
  }
}