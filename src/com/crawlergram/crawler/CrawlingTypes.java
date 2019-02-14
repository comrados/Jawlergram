/*
 * Title: CrawlingTypes.java
 * Project: Jawlergram
 * Creator: Georgii Mikriukov
 * 2019
 */

/*
 * Title: CrawlingTypes.java
 * Project: Jawlergram
 * Creator: Georgii Mikriukov
 * 2019
 */

package com.crawlergram.crawler;

import com.crawlergram.crawler.apimethods.DialogsHistoryMethods;
import com.crawlergram.crawler.apimethods.MediaDownloadMethods;
import com.crawlergram.crawler.output.ConsoleOutputMethods;
import com.crawlergram.crawler.output.FileMethods;
import org.telegram.api.dialog.TLDialog;
import org.telegram.api.message.TLAbsMessage;
import org.telegram.tl.TLObject;
import org.telegram.tl.TLVector;
import com.crawlergram.db.MessageHistoryExclusions;

import static com.crawlergram.db.Constants.MSG_DIAL_PREF;

public class CrawlingTypes {

    /**
     * Writes only messages to DB
     *
     * @param c Crawler instance
     */
    public static void saveOnlyMessagesToDB(CrawlerSettings c) {
        for (TLDialog dialog : c.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), c.chatsHashMap, c.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(c.dbStorage, dialog);
            if (exclusions.exist()) {
                System.out.println("Top DB message: " + exclusions.getMaxId());
                int count = dialog.getTopMessage() - exclusions.getMaxId();
                System.out.println("Downloading at most " + (count > 0 ? count : 0) + " messages");
            }


            //reads full dialog info
            TLObject fullDialog = DialogsHistoryMethods.getFullDialog(c.api, dialog, c.chatsHashMap, c.usersHashMap);
            //writes full dialog info
            c.dbStorage.writeFullDialog(fullDialog, c.chatsHashMap, c.usersHashMap);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, c.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()) {
                absMessages = DialogsHistoryMethods.getWholeMessageHistoryWithExclusions(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, exclusions, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessageHistory(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            }
            System.out.println("Downloaded: " + absMessages.size());
            // writes messages of the dialog to "messages + [dialog_id]" table/collection/etc.
            c.dbStorage.writeTLAbsMessages(absMessages, dialog);

            //reads participants
            TLObject participants = DialogsHistoryMethods.getParticipants(c.api, fullDialog, c.chatsHashMap, c.usersHashMap, c.PARTICIPANTS_LIMIT, c.PARTICIPANTS_FILTER);
            // writes participants of the dialog to "messages + [dialog_id]" table/collection/etc.
            c.dbStorage.writeParticipants(participants, dialog);

            // sleep between transmissions to avoid flood wait
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        // write hashmaps
        System.out.println("Writing obtained users chats, duplicates may occure");
        c.dbStorage.writeUsersHashMap(c.usersHashMap);
        c.dbStorage.writeChatsHashMap(c.chatsHashMap);
        System.out.println("Done");
        System.out.println();
    }

    /**
     * Writes only messages to HDD
     *
     * @param c Crawler instance
     */
    public static void saveOnlyMediaToHDD(CrawlerSettings c) {
        int filesCounter = 0;
        for (TLDialog dialog : c.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), c.chatsHashMap, c.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, c.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            absMessages = DialogsHistoryMethods.getWholeMessageHistory(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            System.out.println("Downloaded: " + absMessages.size());


            for (TLAbsMessage absMessage : absMessages)
                if (filesCounter < c.FILES_LIMIT)
                    if (MediaDownloadMethods.messageDownloadMediaToHDD(c.api, absMessage, c.MAX_FILE_SIZE, c.FILES_PATH) != null)
                        filesCounter++;
                    else break;


            System.out.println("Done");
            System.out.println();
            // sleep between transmissions to avoid flood wait
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * Writes only messages to DB
     *
     * @param c Crawler instance
     */
    public static void saveOnlyMediaToDB(CrawlerSettings c) {
        int filesCounter = 0;
        for (TLDialog dialog : c.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), c.chatsHashMap, c.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(c.dbStorage, dialog);
            if (exclusions.exist()) {
                System.out.println("Top DB message: " + exclusions.getMaxId());
                int count = dialog.getTopMessage() - exclusions.getMaxId();
                System.out.println("Downloading at most " + (count > 0 ? count : 0) + " messages");
            }

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, c.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()) {
                absMessages = DialogsHistoryMethods.getWholeMessageHistoryWithExclusions(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, exclusions, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessageHistory(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            }
            System.out.println("Downloaded: " + absMessages.size());


            for (TLAbsMessage absMessage : absMessages)
                if (filesCounter < c.FILES_LIMIT)
                    if (MediaDownloadMethods.messageDownloadMediaToDB(c.api, c.dbStorage, absMessage, c.MAX_FILE_SIZE) != null)
                        filesCounter++;
                    else break;


            System.out.println("Done");
            System.out.println();
            // sleep between transmissions to avoid flood wait
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * Writes messages and files to HDD
     *
     * @param c Crawler instance
     */
    public static void saveMessagesToDBFilesToHDD(CrawlerSettings c) {
        for (TLDialog dialog : c.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), c.chatsHashMap, c.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(c.dbStorage, dialog);
            if (exclusions.exist()) {
                System.out.println("Top DB message: " + exclusions.getMaxId());
                int count = dialog.getTopMessage() - exclusions.getMaxId();
                System.out.println("Downloading at most " + (count > 0 ? count : 0) + " messages");
            }

            //reads full dialog info
            TLObject fullDialog = DialogsHistoryMethods.getFullDialog(c.api, dialog, c.chatsHashMap, c.usersHashMap);
            //writes full dialog info
            c.dbStorage.writeFullDialog(fullDialog, c.chatsHashMap, c.usersHashMap);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, c.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()) {
                absMessages = DialogsHistoryMethods.getWholeMessageHistoryWithExclusions(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, exclusions, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessageHistory(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            }
            System.out.println("Downloaded: " + absMessages.size());

            // writes messages of the dialog to "messages + [dialog_id]" table/collection/etc.
            c.dbStorage.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            for (TLAbsMessage absMessage : absMessages) {
                String reference = MediaDownloadMethods.messageDownloadMediaToHDD(c.api, absMessage, c.MAX_FILE_SIZE, c.FILES_PATH);
                if (reference != null) {
                    c.dbStorage.writeTLAbsMessageWithReference(absMessage, reference);
                } else {
                    c.dbStorage.writeTLAbsMessage(absMessage);
                }
            }

            //reads participants
            TLObject participants = DialogsHistoryMethods.getParticipants(c.api, fullDialog, c.chatsHashMap, c.usersHashMap, c.PARTICIPANTS_LIMIT, c.PARTICIPANTS_FILTER);
            // writes participants of the dialog to "messages + [dialog_id]" table/collection/etc.
            c.dbStorage.writeParticipants(participants, dialog);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        // write hashmaps
        System.out.println("Writing obtained users chats, duplicates may occure");
        c.dbStorage.writeUsersHashMap(c.usersHashMap);
        c.dbStorage.writeChatsHashMap(c.chatsHashMap);
        System.out.println("Done");
        System.out.println();
    }

    /**
     * Writes only messages to DB
     *
     * @param c Crawler instance
     */
    public static void saveMessagesToDBFilesToDB(CrawlerSettings c) {
        for (TLDialog dialog : c.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), c.chatsHashMap, c.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(c.dbStorage, dialog);
            if (exclusions.exist()) {
                System.out.println("Top DB message: " + exclusions.getMaxId());
                int count = dialog.getTopMessage() - exclusions.getMaxId();
                System.out.println("Downloading at most " + (count > 0 ? count : 0) + " messages");
            }

            //reads full dialog info
            TLObject fullDialog = DialogsHistoryMethods.getFullDialog(c.api, dialog, c.chatsHashMap, c.usersHashMap);
            //writes full dialog info
            c.dbStorage.writeFullDialog(fullDialog, c.chatsHashMap, c.usersHashMap);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, c.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()) {
                absMessages = DialogsHistoryMethods.getWholeMessageHistoryWithExclusions(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, exclusions, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessageHistory(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            }
            System.out.println("Downloaded: " + absMessages.size());

            // writes messages of the dialog to "messages + [dialog_id]" table/collection/etc.
            c.dbStorage.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            for (TLAbsMessage absMessage : absMessages) {
                String reference = MediaDownloadMethods.messageDownloadMediaToDB(c.api, c.dbStorage, absMessage, c.MAX_FILE_SIZE);
                if (reference != null) {
                    c.dbStorage.writeTLAbsMessageWithReference(absMessage, reference);
                } else {
                    c.dbStorage.writeTLAbsMessage(absMessage);
                }
            }

            //reads participants
            TLObject participants = DialogsHistoryMethods.getParticipants(c.api, fullDialog, c.chatsHashMap, c.usersHashMap, c.PARTICIPANTS_LIMIT, c.PARTICIPANTS_FILTER);
            // writes participants of the dialog to "messages + [dialog_id]" table/collection/etc.
            c.dbStorage.writeParticipants(participants, dialog);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        // write hashmaps
        System.out.println("Writing obtained users chats, duplicates may occure");
        c.dbStorage.writeUsersHashMap(c.usersHashMap);
        c.dbStorage.writeChatsHashMap(c.chatsHashMap);
        System.out.println("Done");
        System.out.println();
    }

    /**
     * Writes only voice messages to HDD
     *
     * @param c Crawler instance
     */
    public static void saveOnlyVoiceMessagesToHDD(CrawlerSettings c) {
        int filesCounter = 0;
        for (TLDialog dialog : c.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), c.chatsHashMap, c.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());
            System.out.println();

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, c.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            absMessages = DialogsHistoryMethods.getWholeMessageHistory(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            System.out.println("Downloaded: " + absMessages.size());

            for (TLAbsMessage absMessage : absMessages)
                if (filesCounter < c.FILES_LIMIT)
                    if (MediaDownloadMethods.messageDownloadVoiceMessagesToHDD(c.api, absMessage, c.MAX_FILE_SIZE, c.FILES_PATH) != null)
                        filesCounter++;
                    else break;

            System.out.println("Done");
            System.out.println();
            // sleep between transmissions to avoid flood wait
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * Writes only messages to HDD as CSV-files
     *
     * @param c Crawler instance
     */
    public static void saveOnlyMessagesToHDD(CrawlerSettings c) {
        for (TLDialog dialog : c.dialogs) {

            String fullName = ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), c.chatsHashMap, c.usersHashMap);

            System.out.println();
            System.out.println("Crawling dialog: " + fullName);
            System.out.println("Top message: " + dialog.getTopMessage());

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, c.messagesHashMap);
            TLVector<TLAbsMessage> absMessages = DialogsHistoryMethods.getWholeMessageHistory(c.api, dialog, c.chatsHashMap, c.usersHashMap, topMessage, c.MESSAGES_LIMIT, c.MAX_DATE, c.MIN_DATE);
            System.out.println("Downloaded: " + absMessages.size());

            System.out.println("Writing: " + fullName + ".csv");
            // write here
            FileMethods.writeMessagesToCSV(absMessages, String.valueOf(dialog.getPeer().getId()), c.FILES_PATH, ";");

        }
        // sleep between transmissions to avoid flood wait
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {

        }
        // write hashmaps
        System.out.println("Done");
    }
}