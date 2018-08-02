/*
 * Title: CrawlerMain.java
 * Project: telegramJ
 * Creator: Georgii Mikriukov
 * 2018
 */

/*
 * Connects to The telegram, gets dialogs, saves messages and documents to DB
 */

package com.crawlergram.crawler;

import com.crawlergram.crawler.apicallback.ApiCallbackImplemented;
import com.crawlergram.crawler.apimethods.AuthMethods;
import com.crawlergram.crawler.apimethods.CrawlingMethods;
import com.crawlergram.crawler.apimethods.DialogsHistoryMethods;
import com.crawlergram.crawler.logs.LogMethods;
import com.crawlergram.crawler.output.ConsoleOutputMethods;
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
import com.crawlergram.db.DBStorage;
import com.crawlergram.db.mongo.MongoDBStorage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CrawlerMain {

    // api variables
    private static int APIKEY = 0; // your api keys
    private static String APIHASH = ""; // your api hash
    private static String PHONENUMBER = ""; // your phone number
    private static String API_STATE_FILE = "api.state"; // api state is saved to HDD
    private static String DEVICE_MODEL = ""; // model name
    private static String OS = ""; // os name
    private static String VERSION = ""; // version
    private static String LANG_CODE = ""; // language code
    private static String NAME = "John"; // name (for signing up)
    private static String SURNAME = "Doe"; // surname (for signing up)

    // db variables
    private static String TYPE = ""; // type of storage (at the moment, only "mongodb")
    private static String USERNAME = ""; // db user
    private static String DATABASE_NAME = ""; // db name
    private static String HOST = ""; // db host
    private static Integer PORT = -1; // db port
    private static String GRIDFS_BUCKET_NAME = "fs"; // gridFS bucket
    private static String PASSWORD = ""; // db password

    // crawler variables
    private static Integer MESSAGES_LIMIT = 0; // maximum number of retrieved messages from each dialog (0 if all)
    private static Integer PARTICIPANTS_LIMIT = 0; // maximum number of retrieved participants from each dialog (0 if all)
    private static Integer PARTICIPANTS_FILTER = 0; // participants filter: 0 - recent, 1 - admins, 2 - kicked, 3 - bots, default - recent
    private static Integer MIN_DATE = 0; // min date of message (0 if no limit)
    private static Integer MAX_DATE = 0; // max date of message (0 if no limit)
    private static Integer MAX_FILE_SIZE = 10485760; // Maximum size of downloadable files
    private static String FILES_PATH = "files"; // Path to save downloaded files
    private static Integer FILES_LIMIT = 0; // Maximum amount of files to download

    // other
    private static AbsApiState apiState;
    private static AppInfo appInfo;
    private static ApiCallback apiCallback = new ApiCallbackImplemented();
    private static TelegramApi api;
    private static Map<Integer, TLAbsChat> chatsHashMap = new HashMap<>();
    private static Map<Integer, TLAbsUser> usersHashMap = new HashMap<>();
    private static TLVector<TLDialog> dialogs = new TLVector<>();
    private static Map<Integer, TLAbsMessage> messagesHashMap = new HashMap<>();
    private static DBStorage dbStorage;

    public static void main(String[] args) {

        if (args.length != 4) {
            printHelp("Wrong arguments given");
            System.exit(1);
        } else {
            try {

                Integer type = 0;
                try {
                    type = Integer.valueOf(args[0]);
                } catch (Exception ex){
                    printHelp("Argument <run_type> must be integer");
                    System.exit(1);
                }

                readConfigs(args);

                switch (type){
                    case 1:
                        printErrMessage("Saving only messages to DB");
                        initApiDoAuth();
                        CrawlingMethods.saveOnlyMessages(api, dbStorage, dialogs, chatsHashMap, usersHashMap, messagesHashMap, MESSAGES_LIMIT, PARTICIPANTS_LIMIT, PARTICIPANTS_FILTER, MAX_DATE, MIN_DATE);
                        break;
                    case 2:
                        printErrMessage("Saves messages to DB, files to HDD");
                        initApiDoAuth();
                        CrawlingMethods.saveMessagesToDBFilesToHDD(api, dbStorage, dialogs, chatsHashMap, usersHashMap, messagesHashMap, MESSAGES_LIMIT, PARTICIPANTS_LIMIT, PARTICIPANTS_FILTER, MAX_DATE, MIN_DATE, MAX_FILE_SIZE, FILES_PATH);
                        break;
                    case 3:
                        printErrMessage("Saves messages to DB, files to DB");
                        initApiDoAuth();
                        CrawlingMethods.saveMessagesToDBFilesToDB(api, dbStorage, dialogs, chatsHashMap, usersHashMap, messagesHashMap, MESSAGES_LIMIT, PARTICIPANTS_LIMIT, PARTICIPANTS_FILTER, MAX_DATE, MIN_DATE, MAX_FILE_SIZE);
                        break;
                    case 4:
                        printErrMessage("Saves only files to DB");
                        initApiDoAuth();
                        CrawlingMethods.saveOnlyMediaToDB(api, dbStorage, dialogs, chatsHashMap, usersHashMap, messagesHashMap, MESSAGES_LIMIT, MAX_DATE, MIN_DATE, FILES_LIMIT, MAX_FILE_SIZE);
                        break;
                    case 5:
                        printErrMessage("Saves only files to HDD");
                        initApiDoAuthNoDB();
                        CrawlingMethods.saveOnlyMediaToHDD(api,dialogs, chatsHashMap, usersHashMap, messagesHashMap, MESSAGES_LIMIT, MAX_DATE, MIN_DATE, MAX_FILE_SIZE, FILES_PATH, FILES_LIMIT);
                        break;
                    case 6:
                        printErrMessage("Saves only voice messages to HDD");
                        initApiDoAuthNoDB();
                        CrawlingMethods.saveOnlyVoiceMessagesToHDD(api, dialogs, chatsHashMap, usersHashMap, messagesHashMap, MESSAGES_LIMIT, MAX_DATE, MIN_DATE, MAX_FILE_SIZE, FILES_PATH, FILES_LIMIT);
                        break;
                    default:
                        printHelp("<run_type> argument " + type + " doesn't exist");
                        System.exit(1);

                }
                printErrMessage("Successfully done.");
            } catch (Exception e){
                printHelp(e.getMessage());
                e.printStackTrace();
            }
        }
        System.exit(0);
    }

    /**
     * Wrapper for printing help
     *
     * @param errMsg message
     */
    private static void printHelp(String errMsg){
        System.out.println();
        System.out.println(errMsg);
        System.out.println("Program call form: java -jar Jawlergram.jar <run_type> <api_config> <crawler_cfg> <storage_config>");
        System.out.println("Read README.md to get more information about syntax.");
        System.out.println();
    }

    /**
     * Wrapper for printing messages
     *
     * @param errMsg message
     */
    private static void printErrMessage(String errMsg){
        System.out.println();
        System.out.println(errMsg);
        System.out.println();
    }

    /**
     * Read configs
     *
     * @param args arguments
     */
    private static void readConfigs(String[] args){
        try {
            // api 7 mandatory params
            if (readApiCfg(args[1]) < 7) {
                printErrMessage("Not enough mandatory parameters in API configuration");
                System.exit(1);
            }
            // crawler 0 mandatory params
            readCrawlerCfg(args[2]);
            // storage 6 mandatory params
            if (readStorageCfg(args[3]) < 6) {
                printErrMessage("Not enough mandatory parameters in Storage configuration");
                System.exit(1);
            }
        } catch (Exception e){
            printHelp("Wrong arguments or not enough arguments given. Read hints in configuration files.");
            System.exit(1);
        }
    }

    /**
     * Read api config
     *
     * @param path path to the file
     * @return number of read parameters
     */
    private static int readApiCfg(String path){
        String errMsg = "Can't read API configuration.";
        Set<String> params = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            for (String line; (line = br.readLine()) != null;){
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    String[] words = line.split("=");
                    if (words.length != 2){
                        printHelp(errMsg + "\n\rWrong line:" + line + "\n\rProper line <parameter_name> = <parameter value>");
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
            printHelp(errMsg);
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
    private static int readCrawlerCfg(String path){
        String errMsg = "Can't read Crawler configuration.";
        Set<String> params = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            for (String line; (line = br.readLine()) != null;){
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    String[] words = line.split("=");
                    if (words.length != 2){
                        printHelp(errMsg + "\n\rWrong line:" + line + "\n\rProper line <parameter_name> = <parameter value>");
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
            printHelp(errMsg);
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
    private static int readStorageCfg(String path){
        String errMsg = "Can't read Storage configuration.";
        Set<String> params = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            for (String line; (line = br.readLine()) != null;){
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    String[] words = line.split("=");
                    if (words.length != 2){
                        printHelp(errMsg + "\n\rWrong line:" + line + "\n\rProper line <parameter_name> = <parameter value>");
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
            printHelp(errMsg);
            System.exit(1);
        }
        return params.size();
    }

    private static void initApiDoAuth(){

        switch (TYPE.toLowerCase()){
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

        // output to console
        ConsoleOutputMethods.testChatsHashMapOutputConsole(chatsHashMap);
        ConsoleOutputMethods.testUsersHashMapOutputConsole(usersHashMap);
    }

    private static void initApiDoAuthNoDB(){
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

        // output to console
        ConsoleOutputMethods.testChatsHashMapOutputConsole(chatsHashMap);
        ConsoleOutputMethods.testUsersHashMapOutputConsole(usersHashMap);
    }

}
