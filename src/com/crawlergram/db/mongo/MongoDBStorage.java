/*
 * Title: MongoInterface.java
 * Project: telegramJ
 * Creator: Georgii Mikriukov
 * 2018
 */

package com.crawlergram.db.mongo;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.crawlergram.crawler.output.FileMethods;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.telegram.api.channel.TLChannelParticipants;
import org.telegram.api.channel.participants.*;
import org.telegram.api.chat.*;
import org.telegram.api.chat.channel.TLChannel;
import org.telegram.api.chat.channel.TLChannelForbidden;
import org.telegram.api.chat.channel.TLChannelFull;
import org.telegram.api.chat.invite.*;
import org.telegram.api.chat.participant.chatparticipant.TLAbsChatParticipant;
import org.telegram.api.chat.participant.chatparticipant.TLChatParticipant;
import org.telegram.api.chat.participant.chatparticipant.TLChatParticipantAdmin;
import org.telegram.api.chat.participant.chatparticipant.TLChatParticipantCreator;
import org.telegram.api.chat.participant.chatparticipants.TLChatParticipants;
import org.telegram.api.chat.photo.TLAbsChatPhoto;
import org.telegram.api.chat.photo.TLChatPhoto;
import org.telegram.api.chat.photo.TLChatPhotoEmpty;
import org.telegram.api.dialog.TLDialog;
import org.telegram.api.document.TLAbsDocument;
import org.telegram.api.document.TLDocument;
import org.telegram.api.document.TLDocumentEmpty;
import org.telegram.api.document.attribute.*;
import org.telegram.api.file.location.TLAbsFileLocation;
import org.telegram.api.file.location.TLFileLocation;
import org.telegram.api.file.location.TLFileLocationUnavailable;
import org.telegram.api.game.TLGame;
import org.telegram.api.geo.point.TLAbsGeoPoint;
import org.telegram.api.geo.point.TLGeoPoint;
import org.telegram.api.geo.point.TLGeoPointEmpty;
import org.telegram.api.input.chat.TLAbsInputChannel;
import org.telegram.api.input.chat.TLInputChannel;
import org.telegram.api.input.chat.TLInputChannelEmpty;
import org.telegram.api.message.*;
import org.telegram.api.message.action.*;
import org.telegram.api.message.media.*;
import org.telegram.api.messages.TLMessagesChatFull;
import org.telegram.api.paymentapi.TLPaymentCharge;
import org.telegram.api.paymentapi.TLPaymentRequestedInfo;
import org.telegram.api.paymentapi.TLPostAddress;
import org.telegram.api.peer.TLAbsPeer;
import org.telegram.api.peer.TLPeerChannel;
import org.telegram.api.peer.TLPeerChat;
import org.telegram.api.peer.TLPeerUser;
import org.telegram.api.phone.call.discardreason.*;
import org.telegram.api.photo.TLAbsPhoto;
import org.telegram.api.photo.TLPhoto;
import org.telegram.api.photo.TLPhotoEmpty;
import org.telegram.api.photo.size.TLAbsPhotoSize;
import org.telegram.api.photo.size.TLPhotoSize;
import org.telegram.api.user.TLAbsUser;
import org.telegram.api.user.TLUser;
import org.telegram.api.user.TLUserEmpty;
import org.telegram.api.user.TLUserFull;
import org.telegram.api.webpage.TLAbsWebPage;
import org.telegram.api.webpage.TLWebPage;
import org.telegram.api.webpage.TLWebPageEmpty;
import org.telegram.tl.TLIntVector;
import org.telegram.tl.TLObject;
import org.telegram.tl.TLVector;
import com.crawlergram.db.DBStorage;

import java.io.*;
import java.util.*;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.crawlergram.db.Constants.*;

/**
 * Class for writing and reading data to and from MongoDB.
 */

public class MongoDBStorage implements DBStorage {

    private String user; // the user name
    private String db; // the name of the db in which the user is defined
    private String psw; // the psw
    private String host; // host
    private Integer port; // port
    private MongoCredential credential; // auth info
    private MongoClientOptions options; // client options
    private MongoClient mongoClient; // client instance
    private MongoDatabase database; // db instance
    private GridFSBucket gridFSBucket; // bucket for files
    private MongoCollection<Document> collection; //collection
    private boolean upsert; // upsert into DB? if false - regular write

    public MongoDBStorage(String user, String db, String psw, String host, Integer port, String gridFSBucketName){
        this.user = user;
        this.db = db;
        this.psw = psw;
        this.host = host;
        this.port = port;
        this.credential = MongoCredential.createCredential(user, db, psw.toCharArray());
        this.options = MongoClientOptions.builder().build();
        this.mongoClient = new MongoClient(new ServerAddress(host, port), credential, options);
        this.database = mongoClient.getDatabase(db);
        this.gridFSBucket = GridFSBuckets.create(this.database, gridFSBucketName);
        this.upsert = false;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getPsw() {
        return psw;
    }

    public void setPsw(String psw) {
        this.psw = psw;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public MongoCredential getCredential() {
        return credential;
    }

    public void setCredential(MongoCredential credential) {
        this.credential = credential;
    }

    public MongoClientOptions getOptions() {
        return options;
    }

    public void setOptions(MongoClientOptions options) {
        this.options = options;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public MongoDatabase getDatabase() {
        return database;
    }

    public void setDatabase(MongoDatabase database) {
        this.database = database;
    }

    public GridFSBucket getGridFSBucket() {
        return gridFSBucket;
    }

    public void setGridFSBucket(GridFSBucket gridFSBucket) {
        this.gridFSBucket = gridFSBucket;
    }

    public MongoCollection<Document> getCollection() {
        return collection;
    }

    public void setCollection(MongoCollection<Document> collection) {
        this.collection = collection;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public void setUpsert(boolean upsert) {
        this.upsert = upsert;
    }

    public void setGridFSBucket(String gridFSBucketName) {
        gridFSBucket = GridFSBuckets.create(database, gridFSBucketName);
    }

    public void setCollection(String collName) {
        collection = database.getCollection(collName);
    }

    /**
     * sets target database
     * @param database target database
     */
    @Override
    public void setDatabase(String database) {
        try {
            this.database = mongoClient.getDatabase(database);
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
        }
    }

    /**
     * place to read/write
     * @param target target collection
     */
    @Override
    public void setTarget(String target) {
        try {
            collection = database.getCollection(target);
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
        }
    }

    /**
     * Drops target collection
     * @param target target collection
     */
    @Override
    public void dropTarget(String target) {
        try{
            database.getCollection(target).drop();
        } catch (MongoException e){
            System.err.println(e.getCode() + " " + e.getMessage());
        }
    }

    /**
     * Drops current db
     */
    @Override
    public void dropDatabase() {
        try{
            database.drop();
        } catch (MongoException e){
            System.err.println(e.getCode() + " " + e.getMessage());
        }
    }

    /**
     * writes object to db
     * @param obj object
     */
    @Override
    public void write(Object obj) {
        if (obj != null) {
            if (!isUpsert()) {
                try {
                    collection.insertOne((Document) obj);
                } catch (MongoException e) {
                    if (e.getCode() != 11000)
                        System.err.println(e.getCode() + " " + e.getMessage());
                }
            } else {
                try {
                    Document doc = (Document) obj;
                    collection.updateOne(Filters.eq("_id", doc.get("_id")), new Document("$set", doc), new UpdateOptions().upsert(true));
                } catch (MongoException e) {
                    if (e.getCode() != 11000)
                        System.err.println(e.getCode() + " " + e.getMessage());
                }

            }
        }
    }

    /**
     * Writes full dialog to db
     * @param dial object with dialog (chat/channel/user)
     * @param chatsHashMap prevents unnecessary downloading by getting data from HashMap
     * @param usersHashMap prevents unnecessary downloading by getting data from HashMap
     */
    @Override
    public void writeFullDialog(TLObject dial, Map<Integer, TLAbsChat> chatsHashMap, Map<Integer, TLAbsUser> usersHashMap){
        // set target
        this.setTarget(DIALOGS);
        // write it
        if (dial instanceof TLMessagesChatFull) {
            TLAbsChatFull absChatFull = ((TLMessagesChatFull) dial).getFullChat();
            //check if chat full or channel full
            if (absChatFull instanceof TLChannelFull) {
                this.write(TLObjectsToMongoMethods.tlChannelFullToDocument((TLChannelFull) absChatFull));
            } else if (absChatFull instanceof TLChatFull) {
                this.write(TLObjectsToMongoMethods.tlChatFullToDocument((TLChatFull) absChatFull));
            }
        } else if (dial instanceof TLUserFull) {
            this.write(TLObjectsToMongoMethods.tlUserFullToDocument((TLUserFull) dial));
        }
    }

    /**
     * writes users hashmap to db
     * @param usersHashMap hashmap
     */
    @Override
    public void writeUsersHashMap(Map<Integer, TLAbsUser> usersHashMap) {
        // set target
        this.setTarget(USERS_COL);
        // write
        Set<Integer> keys = usersHashMap.keySet();
        for (Integer key : keys) {
            TLAbsUser absUser = usersHashMap.get(key);
            this.write(TLObjectsToMongoMethods.tlAbsUserToDocument(absUser));
        }
    }

    /**
     * writes chats hashmap to db
     * @param chatsHashMap hashmap
     */
    @Override
    public void writeChatsHashMap(Map<Integer, TLAbsChat> chatsHashMap) {
        // set target
        this.setTarget(CHATS_COL);
        // write
        Set<Integer> keys = chatsHashMap.keySet();
        for (Integer key : keys) {
            TLAbsChat absChat = chatsHashMap.get(key);
            this.write(TLObjectsToMongoMethods.tlAbsChatToDocument(absChat));
        }
    }

    /**
     * writes participants to db
     * @param participants participants
     */
    @Override
    public void writeParticipants(TLObject participants, TLDialog dialog) {
        this.setTarget(PAR_DIAL_PREF + dialog.getPeer().getId());
        if (participants != null){
            if (participants instanceof TLChatParticipants){
                writeChatsParticipants(((TLChatParticipants) participants).getParticipants());
            } else if (participants instanceof TLUserFull){
                this.write(TLObjectsToMongoMethods.tlUserFullToDocument((TLUserFull) participants));
            } else if ((participants instanceof TLChannelParticipants)){
                if (((TLChannelParticipants) participants).getCount() > 0){
                    writeChannelParticipants(((TLChannelParticipants) participants).getParticipants());
                }
            }
        }
    }

    /**
     * Write messages to DB
     * @param absMessages messages
     * @param dialog dialog
     */
    @Override
    public void writeTLAbsMessages(TLVector<TLAbsMessage> absMessages, TLDialog dialog) {
        this.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
        if ((absMessages != null) && (!absMessages.isEmpty())){
            try {
                for (TLAbsMessage absMessage : absMessages) {
                    writeTLAbsMessage(absMessage);
                }
            } catch (MongoException e) {
                System.err.println(e.getCode() + " " + e.getMessage());
            }
        }
    }

    /**
     * Write a single TLAbsMessage to DB
     * @param absMessage abstract message
     */
    @Override
    public void writeTLAbsMessage(TLAbsMessage absMessage){
        if (absMessage instanceof TLMessage){
            this.write(TLObjectsToMongoMethods.tlMessageToDocument((TLMessage) absMessage));
        } else if (absMessage instanceof TLMessageService){
            this.write(TLObjectsToMongoMethods.tlMessageServiceToDocument((TLMessageService) absMessage));
        } else if (absMessage instanceof TLMessageEmpty){
            this.write(new Document("class","MessageEmpty")
                    .append("_id",((TLMessageEmpty) absMessage).getId())
                    .append("chatId", absMessage.getChatId()));
        }
    }

    /**
     * Write messages to DB
     * @param absMessage messages
     * @param filePath path to the downloaded (reference)
     */
    @Override
    public void writeTLAbsMessageWithReference(TLAbsMessage absMessage, String filePath) {
        int id = -1;
        if (absMessage instanceof TLMessage){
            this.write(TLObjectsToMongoMethods.tlMessageToDocumentWithReference((TLMessage) absMessage, filePath));
            id = ((TLMessage) absMessage).getId();
        } else if (absMessage instanceof TLMessageService){
            this.write(TLObjectsToMongoMethods.tlMessageServiceToDocument((TLMessageService) absMessage));
            id = ((TLMessageService) absMessage).getId();
        } else if (absMessage instanceof TLMessageEmpty){
            this.write(new Document("class","MessageEmpty")
                    .append("_id",((TLMessageEmpty) absMessage).getId())
                    .append("chatId", absMessage.getChatId()));
            id = ((TLMessageEmpty) absMessage).getId();
        }
        System.out.println(id);
    }

    /**
     * max id of the message from a particular chat
     */
    @Override
    public Integer getMessageMaxId(TLDialog dialog){
        try {
            this.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            FindIterable<Document> findMax = collection.find().sort(descending("_id")).limit(1);
            Document docMax = findMax.first();
            return docMax != null ? (Integer) docMax.get("_id") : null;
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            return null;
        }
    }

    /**
     * min id of the message from a particular chat (for offset)
     */
    @Override
    public Integer getMessageMinId(TLDialog dialog) {
        try {
            this.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            FindIterable<Document> findMin = collection.find().sort(ascending("_id")).limit(1);
            Document docMin = findMin.first();
            return docMin != null ? (Integer) docMin.get("_id") : null;
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            return null;
        }
    }

    /**
     * date of min id message from a particular chat (for offset)
     */
    @Override
    public Integer getMessageMinIdDate(TLDialog dialog) {
        try {
            this.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            FindIterable<Document> findMin = collection.find().sort(ascending("_id")).limit(1);
            Document docMin = findMin.first();
            return docMin != null ? (Integer) docMin.get("date") : null;
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            return null;
        }
    }

    /**
     * date of max id message from a particular chat (for offset)
     */
    @Override
    public Integer getMessageMaxIdDate(TLDialog dialog) {
        try {
            this.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            FindIterable<Document> findMax = collection.find().sort(descending("_id")).limit(1);
            Document docMax = findMax.first();
            return docMax != null ? (Integer) docMax.get("date") : null;
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            return null;
        }
    }

    /**
     * writes bytes to GridFS
     * @param name filename
     * @param bytes contents
     */
    @Override
    public void writeFile(String name, byte[] bytes) {
        try {
            InputStream inputStream = new ByteArrayInputStream(bytes);
            // file type (last split)
            String[] split = name.split("\\.");
            String type = split[split.length - 1];
            // 100kb chunks
            GridFSUploadOptions options = new GridFSUploadOptions().chunkSizeBytes(100 * 1024).metadata(new Document("type", type));
            gridFSBucket.uploadFromStream(name, inputStream, options);
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
        }
    }

    /**
     * creates single field index
     * @param field indexing field
     * @param type switch: 1 - ascending, -1 - descending, default - ascending
     */
    @Override
    public void createIndex(String field, int type) {
        try {
            switch (type) {
                case 1:
                    collection.createIndex(Indexes.ascending(field));
                    break;
                case -1:
                    collection.createIndex(Indexes.descending(field));
                    break;
                default:
                    collection.createIndex(Indexes.ascending(field));
                    break;
            }
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
        }
    }

    /**
     * creates composite index
     * @param fields indexing fields
     * @param types switch: 1 - ascending, -1 - descending, default - ascending
     */
    @Override
    public void createIndex(List<String> fields, List<Integer> types) {
        try {
            // asc and desc indexes
            List<String> asc = new ArrayList<>();
            List<String> desc = new ArrayList<>();
            //check sizes
            if (fields.size() == types.size()){
                // separate desc and asc
                for (int i = 0; i < types.size(); i++){
                    switch (types.get(i)) {
                        case 1:
                            asc.add(fields.get(i)); break;
                        case -1:
                            desc.add(fields.get(i)); break;
                        default:
                            asc.add(fields.get(i)); break;
                    }
                }
                // if only desc is not empty
                if (asc.isEmpty() && (!desc.isEmpty())){
                    collection.createIndex(Indexes.descending(desc));
                }
                // if only asc is not empty
                if (desc.isEmpty() && (!asc.isEmpty())){
                    collection.createIndex(Indexes.ascending(asc));
                }
                // if asc & desc is not empty
                if ((!asc.isEmpty()) && (!desc.isEmpty())) {
                    collection.createIndex(Indexes.compoundIndex(Indexes.ascending(asc), Indexes.descending(desc)));
                }
            } else {
                System.out.println("UNABLE TO CREATE INDEXES: fields and types have different lengths");
            }
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
        }
    }

    /**
     * saves files from DB to HDD
     * @param path HDD path
     */
    @Override
    public void saveFilesToHDD(String path) {
        List<GridFSFile> files = getDBFilesInfo();
        for (GridFSFile file : files) {
            saveFileToHDD(path, file);
        }
    }

    /**
     * saves files from DB to HDD
     * @param path path
     * @param filePointer file id or another pointer
     */
    @Override
    public void saveFileToHDD(String path, Object filePointer) {
        try {
            GridFSFile file = (GridFSFile) filePointer;
            ObjectId oid = file.getObjectId();
            path += File.separator + file.getFilename();
            FileMethods.checkFilePath(path);
            FileOutputStream fos = new FileOutputStream(path);
            gridFSBucket.downloadToStream(oid, fos);
            fos.close();
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            System.out.println("MONGODB ERROR " + ((GridFSFile) filePointer).getFilename());
        } catch (IOException e){
            System.err.println(e.getMessage());
            System.out.println("OUTPUT STREAM ERROR " + ((GridFSFile) filePointer).getFilename());
        }
    }

    /**
     * gets peer info from database
     * @param id id
     */
    public Document getPeerInfo(Integer id) {
        try {
            this.setTarget("CHATS");
            Document peerInfo = collection.find(eq("_id", id)).first();
            if (peerInfo == null) {
                this.setTarget("USERS");
                peerInfo = collection.find(eq("_id", id)).first();
            }
            return peerInfo;
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            return null;
        }
    }

    /**
     * returns list of existing collections names
     */
    public List<String> getAllCollections() {
        try {
            List<String> colNames = new ArrayList<>();
            MongoIterable<String> collections = database.listCollectionNames();
            for (String collection : collections) {
                colNames.add(collection);
            }
            return colNames;
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            return null;
        }
    }

    /**
     * returns list of existing message collections names
     */
    public List<String> getMessagesCollections() {
        try {
            List<String> colNames = new ArrayList<>();
            MongoIterable<String> collections = database.listCollectionNames();
            for (String collection : collections) {
                if (collection.startsWith("MESSAGES")) {
                    colNames.add(collection);
                }
            }
            return colNames;
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            return null;
        }
    }

    /**
     * returns list of existing participant collections names
     */
    public List<String> getParticipantsCollections() {
        try {
            List<String> colNames = new ArrayList<>();
            MongoIterable<String> collections = database.listCollectionNames();
            for (String collection : collections) {
                if (collection.startsWith("PARTICIPANTS")) {
                    colNames.add(collection);
                }
            }
            return colNames;
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            return null;
        }
    }

    /**
     * returns names of stored files
     */
    public List<GridFSFile> getDBFilesInfo() {
        try {
            List<GridFSFile> files = new LinkedList<>();
            GridFSFindIterable gfsi = gridFSBucket.find();
            for (GridFSFile gfsf : gfsi) {
                files.add(gfsf);
            }
            return files;
        } catch (MongoException e) {
            System.err.println(e.getCode() + " " + e.getMessage());
            return null;
        }
    }

    /**
     * writes channel participants
     * @param vacp participants vector
     */
    private void writeChannelParticipants(TLVector<TLAbsChannelParticipant> vacp){
        for (TLAbsChannelParticipant acp: vacp){
            this.write(TLObjectsToMongoMethods.tlAbsChannelParticipantToDocument(acp));
        }
    }


    /**
     * writes chat participants
     * @param vacp participants vector
     */
    private void writeChatsParticipants(TLVector<TLAbsChatParticipant> vacp){
        for (TLAbsChatParticipant acp: vacp){
            this.write(TLObjectsToMongoMethods.tlAbsChatParticipantToDocument(acp));
        }
    }

}
