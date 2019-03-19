/*
 * Title: CrawlerSettings.java
 * Project: Jawlergram
 * Creator: Georgii Mikriukov
 * 2019
 */

/*
 * Title: Crawler.java
 * Project: Jawlergram
 * Creator: Georgii Mikriukov
 * 2019
 */

package com.crawlergram.crawler;

import com.crawlergram.crawler.apicallback.ApiCallbackImplemented;
import com.crawlergram.crawler.apimethods.AuthMethods;
import com.crawlergram.crawler.apimethods.DialogsHistoryMethods;
import com.crawlergram.crawler.logs.LogMethods;
import com.crawlergram.crawler.output.ConsoleOutputMethods;
import com.crawlergram.db.DBStorage;
import com.crawlergram.db.mongo.MongoDBStorage;
import org.telegram.api.chat.TLAbsChat;
import org.telegram.api.dialog.TLDialog;
import org.telegram.api.engine.ApiCallback;
import org.telegram.api.engine.AppInfo;
import org.telegram.api.engine.TelegramApi;
import org.telegram.api.engine.storage.AbsApiState;
import org.telegram.api.message.TLAbsMessage;
import org.telegram.api.user.TLAbsUser;
import org.telegram.bot.kernel.engine.MemoryApiState;
import org.telegram.tl.TLVector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CrawlerSettings {

    // api variables
    public int APIKEY = 0; // your api keys
    public String APIHASH = ""; // your api hash
    public String PHONENUMBER = ""; // your phone number
    public String API_STATE_FILE = "api.state"; // api state is saved to HDD
    public String DEVICE_MODEL = ""; // model name
    public String OS = ""; // os name
    public String VERSION = ""; // version
    public String LANG_CODE = ""; // language code
    public String NAME = "John"; // name (for signing up)
    public String SURNAME = "Doe"; // surname (for signing up)

    // db variables
    public String TYPE = ""; // type of storage (at the moment, only "mongodb")
    public String USERNAME = ""; // db user
    public String DATABASE_NAME = ""; // db name
    public String HOST = ""; // db host
    public Integer PORT = -1; // db port
    public String GRIDFS_BUCKET_NAME = "fs"; // gridFS bucket
    public String PASSWORD = ""; // db password

    // crawler variables
    public Integer MESSAGES_LIMIT = 0; // maximum number of retrieved messages from each dialog (0 if all)
    public Integer PARTICIPANTS_LIMIT = 0; // maximum number of retrieved participants from each dialog (0 if all)
    public Integer PARTICIPANTS_FILTER = 0; // participants filter: 0 - recent, 1 - admins, 2 - kicked, 3 - bots, default - recent
    public Integer MIN_DATE = 0; // min date of message (0 if no limit)
    public Integer MAX_DATE = 0; // max date of message (0 if no limit)
    public Integer MAX_FILE_SIZE = 8*10*1024*1024; // Maximum size of downloadable files (10 MB default)
    public String FILES_PATH = "files"; // Path to save downloaded files
    public Integer FILES_LIMIT = 0; // Maximum amount of files to download

    // misc
    public AbsApiState apiState;
    public AppInfo appInfo;
    public ApiCallback apiCallback = new ApiCallbackImplemented();
    public TelegramApi api;
    public Map<Integer, TLAbsChat> chatsHashMap = new HashMap<>();
    public Map<Integer, TLAbsUser> usersHashMap = new HashMap<>();
    public TLVector<TLDialog> dialogs = new TLVector<>();
    public Map<Integer, TLAbsMessage> messagesHashMap = new HashMap<>();
    public DBStorage dbStorage;

    private boolean dbFlag = false;

    public int getAPIKEY() {
        return APIKEY;
    }

    public String getAPIHASH() {
        return APIHASH;
    }

    public void setAPIHASH(String APIHASH) {
        this.APIHASH = APIHASH;
    }

    public String getPHONENUMBER() {
        return PHONENUMBER;
    }

    public void setPHONENUMBER(String PHONENUMBER) {
        this.PHONENUMBER = PHONENUMBER;
    }

    public String getAPI_STATE_FILE() {
        return API_STATE_FILE;
    }

    public void setAPI_STATE_FILE(String API_STATE_FILE) {
        this.API_STATE_FILE = API_STATE_FILE;
    }

    public String getDEVICE_MODEL() {
        return DEVICE_MODEL;
    }

    public void setDEVICE_MODEL(String DEVICE_MODEL) {
        this.DEVICE_MODEL = DEVICE_MODEL;
    }

    public String getOS() {
        return OS;
    }

    public void setOS(String OS) {
        this.OS = OS;
    }

    public String getVERSION() {
        return VERSION;
    }

    public void setVERSION(String VERSION) {
        this.VERSION = VERSION;
    }

    public String getLANG_CODE() {
        return LANG_CODE;
    }

    public void setLANG_CODE(String LANG_CODE) {
        this.LANG_CODE = LANG_CODE;
    }

    public String getNAME() {
        return NAME;
    }

    public void setNAME(String NAME) {
        this.NAME = NAME;
    }

    public String getSURNAME() {
        return SURNAME;
    }

    public void setSURNAME(String SURNAME) {
        this.SURNAME = SURNAME;
    }

    public String getTYPE() {
        return TYPE;
    }

    public void setTYPE(String TYPE) {
        this.TYPE = TYPE;
    }

    public String getUSERNAME() {
        return USERNAME;
    }

    public void setUSERNAME(String USERNAME) {
        this.USERNAME = USERNAME;
    }

    public String getDATABASE_NAME() {
        return DATABASE_NAME;
    }

    public void setDATABASE_NAME(String DATABASE_NAME) {
        this.DATABASE_NAME = DATABASE_NAME;
    }

    public String getHOST() {
        return HOST;
    }

    public void setHOST(String HOST) {
        this.HOST = HOST;
    }

    public Integer getPORT() {
        return PORT;
    }

    public void setPORT(Integer PORT) {
        this.PORT = PORT;
    }

    public String getGRIDFS_BUCKET_NAME() {
        return GRIDFS_BUCKET_NAME;
    }

    public void setGRIDFS_BUCKET_NAME(String GRIDFS_BUCKET_NAME) {
        this.GRIDFS_BUCKET_NAME = GRIDFS_BUCKET_NAME;
    }

    public String getPASSWORD() {
        return PASSWORD;
    }

    public void setPASSWORD(String PASSWORD) {
        this.PASSWORD = PASSWORD;
    }

    public Integer getMESSAGES_LIMIT() {
        return MESSAGES_LIMIT;
    }

    public void setMESSAGES_LIMIT(Integer MESSAGES_LIMIT) {
        this.MESSAGES_LIMIT = MESSAGES_LIMIT;
    }

    public Integer getPARTICIPANTS_LIMIT() {
        return PARTICIPANTS_LIMIT;
    }

    public void setPARTICIPANTS_LIMIT(Integer PARTICIPANTS_LIMIT) {
        this.PARTICIPANTS_LIMIT = PARTICIPANTS_LIMIT;
    }

    public Integer getPARTICIPANTS_FILTER() {
        return PARTICIPANTS_FILTER;
    }

    public void setPARTICIPANTS_FILTER(Integer PARTICIPANTS_FILTER) {
        this.PARTICIPANTS_FILTER = PARTICIPANTS_FILTER;
    }

    public Integer getMIN_DATE() {
        return MIN_DATE;
    }

    public void setMIN_DATE(Integer MIN_DATE) {
        this.MIN_DATE = MIN_DATE;
    }

    public Integer getMAX_DATE() {
        return MAX_DATE;
    }

    public void setMAX_DATE(Integer MAX_DATE) {
        this.MAX_DATE = MAX_DATE;
    }

    public Integer getMAX_FILE_SIZE() {
        return MAX_FILE_SIZE;
    }

    public void setMAX_FILE_SIZE(Integer MAX_FILE_SIZE) {
        this.MAX_FILE_SIZE = MAX_FILE_SIZE;
    }

    public String getFILES_PATH() {
        return FILES_PATH;
    }

    public void setFILES_PATH(String FILES_PATH) {
        this.FILES_PATH = FILES_PATH;
    }

    public Integer getFILES_LIMIT() {
        return FILES_LIMIT;
    }

    public void setFILES_LIMIT(Integer FILES_LIMIT) {
        this.FILES_LIMIT = FILES_LIMIT;
    }

    public AbsApiState getApiState() {
        return apiState;
    }

    public void setApiState(AbsApiState apiState) {
        this.apiState = apiState;
    }

    public AppInfo getAppInfo() {
        return appInfo;
    }

    public void setAppInfo(AppInfo appInfo) {
        this.appInfo = appInfo;
    }

    public ApiCallback getApiCallback() {
        return apiCallback;
    }

    public void setApiCallback(ApiCallback apiCallback) {
        this.apiCallback = apiCallback;
    }

    public TelegramApi getApi() {
        return api;
    }

    public void setApi(TelegramApi api) {
        this.api = api;
    }

    public Map<Integer, TLAbsChat> getChatsHashMap() {
        return chatsHashMap;
    }

    public void setChatsHashMap(Map<Integer, TLAbsChat> chatsHashMap) {
        this.chatsHashMap = chatsHashMap;
    }

    public Map<Integer, TLAbsUser> getUsersHashMap() {
        return usersHashMap;
    }

    public void setUsersHashMap(Map<Integer, TLAbsUser> usersHashMap) {
        this.usersHashMap = usersHashMap;
    }

    public TLVector<TLDialog> getDialogs() {
        return dialogs;
    }

    public void setDialogs(TLVector<TLDialog> dialogs) {
        this.dialogs = dialogs;
    }

    public Map<Integer, TLAbsMessage> getMessagesHashMap() {
        return messagesHashMap;
    }

    public void setMessagesHashMap(Map<Integer, TLAbsMessage> messagesHashMap) {
        this.messagesHashMap = messagesHashMap;
    }

    public DBStorage getDbStorage() {
        return dbStorage;
    }

    public void setDbStorage(DBStorage dbStorage) {
        this.dbStorage = dbStorage;
    }

    public boolean isDbFlag() {
        return dbFlag;
    }

    public void setDbFlag(boolean dbFlag) {
        this.dbFlag = dbFlag;
    }

    /**
     * Constructor with dbStorage
     *
     * @param apiCfgPath api cfg-file path
     * @param crawlerCfgPath crawler cfg-file path
     * @param dbCfgPath db cfg-file path
     */
    public CrawlerSettings(String apiCfgPath, String crawlerCfgPath, String dbCfgPath){
        try {
            // api 7 mandatory params
            if (readApiCfg(apiCfgPath) < 7) {
                printErrMessage("Not enough mandatory parameters in API configuration");
                System.exit(1);
            }
            // crawler 0 mandatory params
            readCrawlerCfg(crawlerCfgPath);
            // storage 6 mandatory params
            if (readStorageCfg(dbCfgPath) < 6) {
                printErrMessage("Not enough mandatory parameters in Storage configuration");
                System.exit(1);
            }
            dbFlag = true;
        } catch (Exception e){
            System.err.println("Wrong arguments or not enough arguments given. Read hints in configuration files.");
            System.exit(1);
        }
        initApiDoAuth();
    }

    /**
     * Constructor without dbStorage
     *
     * @param apiCfgPath api cfg-file path
     * @param CrawlerCfgPath crawler cfg-file path
     */
    public CrawlerSettings(String apiCfgPath, String CrawlerCfgPath){
        try {
            // api 7 mandatory params
            if (readApiCfg(apiCfgPath) < 7) {
                printErrMessage("Not enough mandatory parameters in API configuration");
                System.exit(1);
            }
            // crawler 0 mandatory params
            readCrawlerCfg(CrawlerCfgPath);
            dbFlag = true;
        } catch (Exception e){
            System.err.println("Wrong arguments or not enough arguments given. Read hints in configuration files.");
            System.exit(1);
        }
        initApiDoAuth();
    }

    /**
     * Read api config
     *
     * @param path path to the file
     * @return number of read parameters
     */
    private int readApiCfg(String path){
        String errMsg = "Can't read API configuration.";
        Set<String> params = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            for (String line; (line = br.readLine()) != null;){
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    String[] words = line.split("=");
                    if (words.length != 2){
                        System.exit(1);
                    } else {
                        String parName = words[0].trim();
                        String parValue = words[1].trim();
                        switch (parName.toLowerCase()){
                            case "device_model":
                                DEVICE_MODEL = parValue;
                                params.add(parName);
                                break;
                            case "os":
                                OS = parValue;
                                params.add(parName);
                                break;
                            case "version":
                                VERSION = parValue;
                                params.add(parName);
                                break;
                            case "lang_code":
                                LANG_CODE = parValue;
                                params.add(parName);
                                break;
                            case "api_state_file":
                                API_STATE_FILE = parValue;
                                break;
                            case "name":
                                NAME = parValue;
                                break;
                            case "surname":
                                SURNAME = parValue;
                                break;
                            case "apikey":
                                try {
                                    APIKEY = Integer.valueOf(parValue);
                                    params.add(parName);
                                } catch (Exception e){
                                    printErrMessage(errMsg + "\n\rAPIKEY must be integer. Current value " + parValue);
                                    System.exit(1);
                                }
                                break;
                            case "apihash":
                                APIHASH = parValue;
                                params.add(parName);
                                break;
                            case "phonenumber":
                                PHONENUMBER = parValue;
                                params.add(parName);
                                break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.exit(1);
        }
        return params.size();
    }

    /**
     * Read crawler config
     *
     * @param path path to the file
     * @return number of read parameters
     */
    private int readCrawlerCfg(String path){
        String errMsg = "Can't read Crawler configuration.";
        Set<String> params = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            for (String line; (line = br.readLine()) != null;){
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    String[] words = line.split("=");
                    if (words.length != 2){
                        System.exit(1);
                    } else {
                        String parName = words[0].trim();
                        String parValue = words[1].trim();
                        switch (parName.toLowerCase()){
                            case "messages_limit":
                                try {
                                    MESSAGES_LIMIT = Integer.valueOf(parValue);
                                    params.add(parName);
                                } catch (Exception e){
                                    printErrMessage(errMsg + "\n\rMESSAGES_LIMIT must be integer. Current value " + parValue);
                                    System.exit(1);
                                }
                                break;
                            case "participants_limit":
                                try {
                                    PARTICIPANTS_LIMIT = Integer.valueOf(parValue);
                                    params.add(parName);
                                } catch (Exception e){
                                    printErrMessage(errMsg + "\n\rPARTICIPANTS_LIMIT must be integer. Current value " + parValue);
                                    System.exit(1);
                                }
                                break;
                            case "participants_filter":
                                try {
                                    PARTICIPANTS_FILTER = Integer.valueOf(parValue);
                                    params.add(parName);
                                } catch (Exception e){
                                    printErrMessage(errMsg + "\n\rPARTICIPANTS_FILTER must be integer. Current value " + parValue);
                                    System.exit(1);
                                }
                                break;
                            case "min_date":
                                try {
                                    MIN_DATE = Integer.valueOf(parValue);
                                    params.add(parName);
                                } catch (Exception e){
                                    printErrMessage(errMsg + "\n\rMIN_DATE must be integer. Current value " + parValue);
                                    System.exit(1);
                                }
                                break;
                            case "max_date":
                                try {
                                    MAX_DATE = Integer.valueOf(parValue);
                                    params.add(parName);
                                } catch (Exception e){
                                    printErrMessage(errMsg + "\n\rMAX_DATE must be integer. Current value " + parValue);
                                    System.exit(1);
                                }
                                break;
                            case "max_file_size":
                                try {
                                    MAX_FILE_SIZE = Integer.valueOf(parValue);
                                    params.add(parName);
                                } catch (Exception e){
                                    printErrMessage(errMsg + "\n\rMAX_FILE_SIZE must be integer. Current value " + parValue);
                                    System.exit(1);
                                }
                                break;
                            case "files_path":
                                FILES_PATH = parValue;
                                params.add(parName);
                                break;
                            case "files_limit":
                                try {
                                    FILES_LIMIT = Integer.valueOf(parValue);
                                    params.add(parName);
                                } catch (Exception e){
                                    printErrMessage(errMsg + "\n\rFILES_LIMIT must be integer. Current value " + parValue);
                                    System.exit(1);
                                }
                                break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.exit(1);
        }
        return params.size();
    }

    /**
     * Read storage config
     *
     * @param path path to the file
     * @return number of read parameters
     */
    private int readStorageCfg(String path){
        String errMsg = "Can't read Storage configuration.";
        Set<String> params = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            for (String line; (line = br.readLine()) != null;){
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    String[] words = line.split("=");
                    if (words.length != 2){
                        System.exit(1);
                    } else {
                        String parName = words[0].trim();
                        String parValue = words[1].trim();
                        switch (parName.toLowerCase()){
                            case "type":
                                TYPE = parValue;
                                params.add(parName);
                                break;
                            case "username":
                                USERNAME = parValue;
                                params.add(parName);
                                break;
                            case "database_name":
                                DATABASE_NAME = parValue;
                                params.add(parName);
                                break;
                            case "host":
                                HOST = parValue;
                                params.add(parName);
                                break;
                            case "port":
                                try {
                                    PORT = Integer.valueOf(parValue);
                                    params.add(parName);
                                } catch (Exception e){
                                    printErrMessage(errMsg + "\n\rPORT must be integer. Current value " + parValue);
                                    System.exit(1);
                                }
                                break;
                            case "gridfs_bucket_name":
                                GRIDFS_BUCKET_NAME = parValue;
                                break;
                            case "password":
                                PASSWORD = parValue;
                                params.add(parName);
                                break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.exit(1);
        }
        return params.size();
    }

    private void initApiDoAuth() {

        if (dbFlag)
            switch (TYPE.toLowerCase()) {
                case "mongodb":
                    dbStorage = new MongoDBStorage(USERNAME, DATABASE_NAME, PASSWORD, HOST, PORT, GRIDFS_BUCKET_NAME);
                    break;
                default:
                    printErrMessage("This storage " + TYPE + " is not implemented. Read storage.cfg");
                    System.exit(1);
            }

        //register loggers (registration is preferable, otherwise - output will be in console)
        LogMethods.registerLogs("logs", false);

        // api state
        apiState = new MemoryApiState(API_STATE_FILE);

        // app info set
        appInfo = new AppInfo(APIKEY, DEVICE_MODEL, OS, VERSION, LANG_CODE);

        // init api
        api = new TelegramApi(apiState, appInfo, apiCallback);

        // set api state
        AuthMethods.setApiState(api, apiState);

        // do auth
        AuthMethods.auth(api, apiState, APIKEY, APIHASH, PHONENUMBER, NAME, SURNAME);

        // get all dialogs of user (telegram returns 100 dialogs at maximum, getting by slices)
        DialogsHistoryMethods.getDialogsChatsUsers(api, dialogs, chatsHashMap, usersHashMap, messagesHashMap);

        // output user dialogs
        for (TLDialog dialog: dialogs){
            System.out.println(ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), chatsHashMap, usersHashMap));
        }
    }

    /**
     * Wrapper for messages printing
     *
     * @param errMsg message
     */
    private static void printErrMessage(String errMsg){
        System.out.println();
        System.out.println(errMsg);
        System.out.println();
    }

}
