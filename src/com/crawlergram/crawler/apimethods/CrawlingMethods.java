/*
 * Title: messagesAndMediaToDB.java
 * Project: telegramJ
 * Creator: Georgii Mikriukov
 * 2018
 */

package com.crawlergram.crawler.apimethods;

import com.crawlergram.crawler.output.ConsoleOutputMethods;
import com.crawlergram.crawler.structures.CrawlerMaps;
import org.telegram.api.chat.TLAbsChat;
import org.telegram.api.dialog.TLDialog;
import org.telegram.api.engine.TelegramApi;
import org.telegram.api.message.TLAbsMessage;
import org.telegram.api.user.TLAbsUser;
import org.telegram.tl.TLObject;
import org.telegram.tl.TLVector;
import com.crawlergram.db.DBStorage;
import com.crawlergram.db.MessageHistoryExclusions;

import java.util.Map;

import static com.crawlergram.db.Constants.MSG_DIAL_PREF;

public class CrawlingMethods {

    /**
     * Writes only messages to DB
     * @param	api  TelegramApi instance for RPC request
     * @param   dbStorage   database instance
     * @param   dialogs dialogs TLVector
     * @param   chatsHashMap    chats hashmap
     * @param   usersHashMap    users hashmap
     * @param   messagesHashMap top messages
     * @param   msgLimit   maximum number of retrieved messages from each dialog (0 if all )
     * @param   parLimit   maximum number of retrieved participants from each dialog (0 if all)
     * @param   filter  participants filter: 0 - recent, 1 - admins, 2 - kicked, 3 - bots, default - recent
     * @param   maxDate max date of diapason for saving
     * @param   minDate min date of diapason for saving
     */
    public static void saveOnlyMessages(TelegramApi api, DBStorage dbStorage, TLVector<TLDialog> dialogs,
                                        Map<Integer, TLAbsChat> chatsHashMap,
                                        Map<Integer, TLAbsUser> usersHashMap,
                                        Map<Integer, TLAbsMessage> messagesHashMap,
                                        int msgLimit, int parLimit, int filter, int maxDate, int minDate) {
        for (TLDialog dialog : dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.testOutHashMapObjectByKey(dialog.getPeer().getId(), chatsHashMap, usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(dbStorage, dialog);

            //reads full dialog info
            TLObject fullDialog = DialogsHistoryMethods.getFullDialog(api, dialog, chatsHashMap, usersHashMap);
            //writes full dialog info
            dbStorage.writeFullDialog(fullDialog, chatsHashMap, usersHashMap);

            //reads participants
            TLObject participants = DialogsHistoryMethods.getParticipants(api, fullDialog, chatsHashMap, usersHashMap, parLimit, filter);
            // writes participants of the dialog to "messages + [dialog_id]" table/collection/etc.
            dbStorage.writeParticipants(participants, dialog);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()){
                absMessages = DialogsHistoryMethods.getWholeMessagesHistoryWithExclusions(api, dialog, chatsHashMap, usersHashMap, topMessage, exclusions, msgLimit, maxDate, minDate);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessagesHistory(api, dialog, chatsHashMap, usersHashMap, topMessage, msgLimit, maxDate, minDate);
            }
            // writes messages of the dialog to "messages + [dialog_id]" table/collection/etc.
            dbStorage.writeTLAbsMessages(absMessages, dialog);

            // sleep between transmissions to avoid flood wait
            try {Thread.sleep(1000);} catch (InterruptedException ignored) {}
        }
        // write hashmaps
        System.out.println("Writing obtained users chats, duplicates may occure");
        dbStorage.writeUsersHashMap(usersHashMap);
        dbStorage.writeChatsHashMap(chatsHashMap);
        System.out.println("Done");
        System.out.println();
    }

    /**
     * Writes only messages to HDD
     * @param	api  TelegramApi instance for RPC request
     * @param   dialogs dialogs TLVector
     * @param   chatsHashMap    chats hashmap
     * @param   usersHashMap    users hashmap
     * @param   messagesHashMap top messages
     * @param   msgLimit   maximum number of retrieved messages from each dialog (0 if all )
     * @param   maxDate max date of diapason for saving
     * @param   minDate min date of diapason for saving
     * @param   path    file system path
     * @param   maxFiles maximum nuber of downloaded files
     */
    public static void saveOnlyMediaToHDD(TelegramApi api, TLVector<TLDialog> dialogs,
                                          Map<Integer, TLAbsChat> chatsHashMap,
                                          Map<Integer, TLAbsUser> usersHashMap,
                                          Map<Integer, TLAbsMessage> messagesHashMap,
                                          int msgLimit, int maxDate, int minDate,
                                          int maxSize, String path, int maxFiles) {
        int filesCounter = 0;
        for (TLDialog dialog : dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.testOutHashMapObjectByKey(dialog.getPeer().getId(), chatsHashMap, usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            absMessages = DialogsHistoryMethods.getWholeMessagesHistory(api, dialog, chatsHashMap, usersHashMap, topMessage, msgLimit, maxDate, minDate);

            if (filesCounter < maxFiles) {
                for (TLAbsMessage absMessage : absMessages) {
                    MediaDownloadMethods.messageDownloadMediaToHDD(api, absMessage, maxSize, path);
                    filesCounter++;
                }
            }

            System.out.println("Done");
            System.out.println();
            // sleep between transmissions to avoid flood wait
            try {Thread.sleep(1000);} catch (InterruptedException ignored) {}
        }
    }

    /**
     * Writes only messages to DB
     * @param	api  TelegramApi instance for RPC request
     * @param   dbStorage   database instance
     * @param   dialogs dialogs TLVector
     * @param   chatsHashMap    chats hashmap
     * @param   usersHashMap    users hashmap
     * @param   messagesHashMap top messages
     * @param   msgLimit   maximum number of retrieved messages from each dialog (0 if all )
     * @param   maxDate max date of diapason for saving
     * @param   minDate min date of diapason for saving
     * @param   maxFiles maximum nuber of downloaded files
     */
    public static void saveOnlyMediaToDB(TelegramApi api, DBStorage dbStorage, TLVector<TLDialog> dialogs,
                                          Map<Integer, TLAbsChat> chatsHashMap,
                                          Map<Integer, TLAbsUser> usersHashMap,
                                          Map<Integer, TLAbsMessage> messagesHashMap,
                                          int msgLimit, int maxDate, int minDate, int maxFiles) {
        int filesCounter = 0;
        for (TLDialog dialog : dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.testOutHashMapObjectByKey(dialog.getPeer().getId(), chatsHashMap, usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(dbStorage, dialog);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()){
                absMessages = DialogsHistoryMethods.getWholeMessagesHistoryWithExclusions(api, dialog, chatsHashMap, usersHashMap, topMessage, exclusions, msgLimit, maxDate, minDate);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessagesHistory(api, dialog, chatsHashMap, usersHashMap, topMessage, msgLimit, maxDate, minDate);
            }

            if (filesCounter < maxFiles) {
                for (TLAbsMessage absMessage : absMessages) {
                    MediaDownloadMethods.messageDownloadMediaToDB(api, dbStorage, absMessage, maxDate);
                    filesCounter++;
                }
            }

            System.out.println("Done");
            System.out.println();
            // sleep between transmissions to avoid flood wait
            try {Thread.sleep(1000);} catch (InterruptedException ignored) {}
        }
    }

    /**
     * Writes messages and files to HDD
     * @param	api  TelegramApi instance for RPC request
     * @param   dbStorage   database instance
     * @param   dialogs dialogs TLVector
     * @param   chatsHashMap    chats hashmap
     * @param   usersHashMap    users hashmap
     * @param   messagesHashMap top messages
     * @param   msgLimit   maximum number of retrieved messages from each dialog (0 if all )
     * @param   parLimit   maximum number of retrieved participants from each dialog (0 if all)
     * @param   filter  participants filter: 0 - recent, 1 - admins, 2 - kicked, 3 - bots, default - recent
     * @param   maxDate max date of diapason for saving
     * @param   minDate min date of diapason for saving
     * @param   maxSize max allowed size of file to download
     * @param   path    file system path
     */
    public static void saveMessagesToDBFilesToHDD(TelegramApi api, DBStorage dbStorage, TLVector<TLDialog> dialogs,
                                            Map<Integer, TLAbsChat> chatsHashMap,
                                            Map<Integer, TLAbsUser> usersHashMap,
                                            Map<Integer, TLAbsMessage> messagesHashMap,
                                            int msgLimit, int parLimit, int filter, int maxDate,
                                            int minDate, int maxSize, String path) {
        for (TLDialog dialog : dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.testOutHashMapObjectByKey(dialog.getPeer().getId(), chatsHashMap, usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(dbStorage, dialog);

            //reads full dialog info
            TLObject fullDialog = DialogsHistoryMethods.getFullDialog(api, dialog, chatsHashMap, usersHashMap);
            //writes full dialog info
            dbStorage.writeFullDialog(fullDialog, chatsHashMap, usersHashMap);

            //reads participants
            TLObject participants = DialogsHistoryMethods.getParticipants(api, fullDialog, chatsHashMap, usersHashMap, parLimit, filter);
            // writes participants of the dialog to "messages + [dialog_id]" table/collection/etc.
            dbStorage.writeParticipants(participants, dialog);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()){
                absMessages = DialogsHistoryMethods.getWholeMessagesHistoryWithExclusions(api, dialog, chatsHashMap, usersHashMap, topMessage, exclusions, msgLimit, maxDate, minDate);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessagesHistory(api, dialog, chatsHashMap, usersHashMap, topMessage, msgLimit, maxDate, minDate);
            }

            // writes messages of the dialog to "messages + [dialog_id]" table/collection/etc.
            dbStorage.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            for (TLAbsMessage absMessage: absMessages){
                String reference = MediaDownloadMethods.messageDownloadMediaToHDD(api, absMessage, maxSize, path);
                if (reference != null){
                    dbStorage.writeTLAbsMessageWithReference(absMessage, reference);
                } else {
                    dbStorage.writeTLAbsMessage(absMessage);
                }
            }

            try {Thread.sleep(1000);} catch (InterruptedException ignored) {}
        }
        // write hashmaps
        System.out.println("Writing obtained users chats, duplicates may occure");
        dbStorage.writeUsersHashMap(usersHashMap);
        dbStorage.writeChatsHashMap(chatsHashMap);
        System.out.println("Done");
        System.out.println();
    }

    /**
     * Writes only messages to DB
     * @param	api  TelegramApi instance for RPC request
     * @param   dbStorage   database instance
     * @param   dialogs dialogs TLVector
     * @param   chatsHashMap    chats hashmap
     * @param   usersHashMap    users hashmap
     * @param   messagesHashMap top messages
     * @param   msgLimit   maximum number of retrieved messages from each dialog (0 if all )
     * @param   parLimit   maximum number of retrieved participants from each dialog (0 if all)
     * @param   filter  participants filter: 0 - recent, 1 - admins, 2 - kicked, 3 - bots, default - recent
     * @param   maxDate max date of diapason for saving
     * @param   minDate min date of diapason for saving
     * @param   maxSize max allowed size of file to download
     */
    public static void saveMessagesToDBFilesToDB(TelegramApi api, DBStorage dbStorage, TLVector<TLDialog> dialogs,
                                                  Map<Integer, TLAbsChat> chatsHashMap,
                                                  Map<Integer, TLAbsUser> usersHashMap,
                                                  Map<Integer, TLAbsMessage> messagesHashMap,
                                                  int msgLimit, int parLimit, int filter,
                                                 int maxDate, int minDate, int maxSize) {
        for (TLDialog dialog : dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.testOutHashMapObjectByKey(dialog.getPeer().getId(), chatsHashMap, usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(dbStorage, dialog);

            //reads full dialog info
            TLObject fullDialog = DialogsHistoryMethods.getFullDialog(api, dialog, chatsHashMap, usersHashMap);
            //writes full dialog info
            dbStorage.writeFullDialog(fullDialog, chatsHashMap, usersHashMap);

            //reads participants
            TLObject participants = DialogsHistoryMethods.getParticipants(api, fullDialog, chatsHashMap, usersHashMap, parLimit, filter);
            // writes participants of the dialog to "messages + [dialog_id]" table/collection/etc.
            dbStorage.writeParticipants(participants, dialog);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()){
                absMessages = DialogsHistoryMethods.getWholeMessagesHistoryWithExclusions(api, dialog, chatsHashMap, usersHashMap, topMessage, exclusions, msgLimit, maxDate, minDate);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessagesHistory(api, dialog, chatsHashMap, usersHashMap, topMessage, msgLimit, maxDate, minDate);
            }

            // writes messages of the dialog to "messages + [dialog_id]" table/collection/etc.
            dbStorage.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            for (TLAbsMessage absMessage: absMessages){
                String reference = MediaDownloadMethods.messageDownloadMediaToDB(api, dbStorage, absMessage, maxSize);
                if (reference != null){
                    dbStorage.writeTLAbsMessageWithReference(absMessage, reference);
                } else {
                    dbStorage.writeTLAbsMessage(absMessage);
                }
            }

            try {Thread.sleep(1000);} catch (InterruptedException ignored) {}
        }
        // write hashmaps
        System.out.println("Writing obtained users chats, duplicates may occure");
        dbStorage.writeUsersHashMap(usersHashMap);
        dbStorage.writeChatsHashMap(chatsHashMap);
        System.out.println("Done");
        System.out.println();
    }

    /**
     * Writes only voice messages to HDD
     * @param	api  TelegramApi instance for RPC request
     * @param   dialogs dialogs TLVector
     * @param   chatsHashMap    chats hashmap
     * @param   usersHashMap    users hashmap
     * @param   messagesHashMap top messages
     * @param   msgLimit   maximum number of retrieved messages from each dialog (0 if all )
     * @param   maxDate max date of diapason for saving
     * @param   minDate min date of diapason for saving
     * @param   path    file system path
     * @param   maxFiles maximum nuber of downloaded files
     */
    public static void saveOnlyVoiceMessagesToHDD(TelegramApi api, TLVector<TLDialog> dialogs,
                                          Map<Integer, TLAbsChat> chatsHashMap, Map<Integer, TLAbsUser> usersHashMap,
                                          Map<Integer, TLAbsMessage> messagesHashMap, int msgLimit, int maxDate,
                                          int minDate, int maxSize, String path, int maxFiles) {
        int filesCounter = 0;
        for (TLDialog dialog : dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.testOutHashMapObjectByKey(dialog.getPeer().getId(), chatsHashMap, usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());
            System.out.println();

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            absMessages = DialogsHistoryMethods.getWholeMessagesHistory(api, dialog, chatsHashMap, usersHashMap, topMessage, msgLimit, maxDate, minDate);

            if (filesCounter < maxFiles){
                for (TLAbsMessage absMessage: absMessages){
                    String reference = MediaDownloadMethods.messageDownloadVoiceMessagesToHDD(api, absMessage, maxSize, path);
                    if (reference != null && !reference.isEmpty()){
                        filesCounter++;
                    }
                }
            }

            System.out.println("Done");
            System.out.println();
            // sleep between transmissions to avoid flood wait
            try {Thread.sleep(1000);} catch (InterruptedException ignored) {}
        }
    }

}
