/*
 * Title: TelegramCrawler.java
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

public class TelegramCrawler {

    private CrawlerSettings settings;

    public TelegramCrawler(CrawlerSettings settings){
        this.settings = settings;
    }

    public CrawlerSettings getSettings() {
        return settings;
    }

    public void setSettings(CrawlerSettings settings) {
        this.settings = settings;
    }

    /**
     * Writes only messages to DB
     */
    public void saveOnlyMessagesToDB() {
        for (TLDialog dialog : settings.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), settings.chatsHashMap, settings.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(settings.dbStorage, dialog);
            if (exclusions.exist()) {
                System.out.println("Top DB message: " + exclusions.getMaxId());
                int count = dialog.getTopMessage() - exclusions.getMaxId();
                System.out.println("Downloading at most " + (count > 0 ? count : 0) + " messages");
            }


            //reads full dialog info
            TLObject fullDialog = DialogsHistoryMethods.getFullDialog(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap);
            //writes full dialog info
            settings.dbStorage.writeFullDialog(fullDialog, settings.chatsHashMap, settings.usersHashMap);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, settings.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()) {
                absMessages = DialogsHistoryMethods.getWholeMessageHistoryWithExclusions(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, exclusions, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessageHistory(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            }
            System.out.println("Downloaded: " + absMessages.size());
            // writes messages of the dialog to "messages + [dialog_id]" table/collection/etc.
            settings.dbStorage.writeTLAbsMessages(absMessages, dialog);

            //reads participants
            TLObject participants = DialogsHistoryMethods.getParticipants(settings.api, fullDialog, settings.chatsHashMap, settings.usersHashMap, settings.PARTICIPANTS_LIMIT, settings.PARTICIPANTS_FILTER);
            // writes participants of the dialog to "messages + [dialog_id]" table/collection/etc.
            settings.dbStorage.writeParticipants(participants, dialog);

            // sleep between transmissions to avoid flood wait
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        // write hashmaps
        System.out.println("Writing obtained users chats, duplicates may occure");
        settings.dbStorage.writeUsersHashMap(settings.usersHashMap);
        settings.dbStorage.writeChatsHashMap(settings.chatsHashMap);
        System.out.println("Done");
        System.out.println();
    }

    /**
     * Writes only messages to HDD
     */
    public void saveOnlyMediaToHDD() {
        int filesCounter = 0;
        for (TLDialog dialog : settings.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), settings.chatsHashMap, settings.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, settings.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            absMessages = DialogsHistoryMethods.getWholeMessageHistory(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            System.out.println("Downloaded: " + absMessages.size());


            for (TLAbsMessage absMessage : absMessages)
                if (filesCounter < settings.FILES_LIMIT)
                    if (MediaDownloadMethods.messageDownloadMediaToHDD(settings.api, absMessage, settings.MAX_FILE_SIZE, settings.FILES_PATH) != null)
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
     */
    public void saveOnlyMediaToDB() {
        int filesCounter = 0;
        for (TLDialog dialog : settings.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), settings.chatsHashMap, settings.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(settings.dbStorage, dialog);
            if (exclusions.exist()) {
                System.out.println("Top DB message: " + exclusions.getMaxId());
                int count = dialog.getTopMessage() - exclusions.getMaxId();
                System.out.println("Downloading at most " + (count > 0 ? count : 0) + " messages");
            }

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, settings.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()) {
                absMessages = DialogsHistoryMethods.getWholeMessageHistoryWithExclusions(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, exclusions, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessageHistory(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            }
            System.out.println("Downloaded: " + absMessages.size());


            for (TLAbsMessage absMessage : absMessages)
                if (filesCounter < settings.FILES_LIMIT)
                    if (MediaDownloadMethods.messageDownloadMediaToDB(settings.api, settings.dbStorage, absMessage, settings.MAX_FILE_SIZE) != null)
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
     */
    public void saveMessagesToDBFilesToHDD() {
        for (TLDialog dialog : settings.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), settings.chatsHashMap, settings.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(settings.dbStorage, dialog);
            if (exclusions.exist()) {
                System.out.println("Top DB message: " + exclusions.getMaxId());
                int count = dialog.getTopMessage() - exclusions.getMaxId();
                System.out.println("Downloading at most " + (count > 0 ? count : 0) + " messages");
            }

            //reads full dialog info
            TLObject fullDialog = DialogsHistoryMethods.getFullDialog(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap);
            //writes full dialog info
            settings.dbStorage.writeFullDialog(fullDialog, settings.chatsHashMap, settings.usersHashMap);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, settings.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()) {
                absMessages = DialogsHistoryMethods.getWholeMessageHistoryWithExclusions(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, exclusions, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessageHistory(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            }
            System.out.println("Downloaded: " + absMessages.size());

            // writes messages of the dialog to "messages + [dialog_id]" table/collection/etc.
            settings.dbStorage.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            for (TLAbsMessage absMessage : absMessages) {
                String reference = MediaDownloadMethods.messageDownloadMediaToHDD(settings.api, absMessage, settings.MAX_FILE_SIZE, settings.FILES_PATH);
                if (reference != null) {
                    settings.dbStorage.writeTLAbsMessageWithReference(absMessage, reference);
                } else {
                    settings.dbStorage.writeTLAbsMessage(absMessage);
                }
            }

            //reads participants
            TLObject participants = DialogsHistoryMethods.getParticipants(settings.api, fullDialog, settings.chatsHashMap, settings.usersHashMap, settings.PARTICIPANTS_LIMIT, settings.PARTICIPANTS_FILTER);
            // writes participants of the dialog to "messages + [dialog_id]" table/collection/etc.
            settings.dbStorage.writeParticipants(participants, dialog);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        // write hashmaps
        System.out.println("Writing obtained users chats, duplicates may occure");
        settings.dbStorage.writeUsersHashMap(settings.usersHashMap);
        settings.dbStorage.writeChatsHashMap(settings.chatsHashMap);
        System.out.println("Done");
        System.out.println();
    }

    /**
     * Writes only messages to DB
     */
    public void saveMessagesToDBFilesToDB() {
        for (TLDialog dialog : settings.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), settings.chatsHashMap, settings.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());

            MessageHistoryExclusions exclusions = new MessageHistoryExclusions(settings.dbStorage, dialog);
            if (exclusions.exist()) {
                System.out.println("Top DB message: " + exclusions.getMaxId());
                int count = dialog.getTopMessage() - exclusions.getMaxId();
                System.out.println("Downloading at most " + (count > 0 ? count : 0) + " messages");
            }

            //reads full dialog info
            TLObject fullDialog = DialogsHistoryMethods.getFullDialog(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap);
            //writes full dialog info
            settings.dbStorage.writeFullDialog(fullDialog, settings.chatsHashMap, settings.usersHashMap);

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, settings.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            if (exclusions.exist()) {
                absMessages = DialogsHistoryMethods.getWholeMessageHistoryWithExclusions(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, exclusions, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            } else {
                absMessages = DialogsHistoryMethods.getWholeMessageHistory(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            }
            System.out.println("Downloaded: " + absMessages.size());

            // writes messages of the dialog to "messages + [dialog_id]" table/collection/etc.
            settings.dbStorage.setTarget(MSG_DIAL_PREF + dialog.getPeer().getId());
            for (TLAbsMessage absMessage : absMessages) {
                String reference = MediaDownloadMethods.messageDownloadMediaToDB(settings.api, settings.dbStorage, absMessage, settings.MAX_FILE_SIZE);
                if (reference != null) {
                    settings.dbStorage.writeTLAbsMessageWithReference(absMessage, reference);
                } else {
                    settings.dbStorage.writeTLAbsMessage(absMessage);
                }
            }

            //reads participants
            TLObject participants = DialogsHistoryMethods.getParticipants(settings.api, fullDialog, settings.chatsHashMap, settings.usersHashMap, settings.PARTICIPANTS_LIMIT, settings.PARTICIPANTS_FILTER);
            // writes participants of the dialog to "messages + [dialog_id]" table/collection/etc.
            settings.dbStorage.writeParticipants(participants, dialog);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        // write hashmaps
        System.out.println("Writing obtained users chats, duplicates may occure");
        settings.dbStorage.writeUsersHashMap(settings.usersHashMap);
        settings.dbStorage.writeChatsHashMap(settings.chatsHashMap);
        System.out.println("Done");
        System.out.println();
    }

    /**
     * Writes only voice messages to HDD
     */
    public void saveOnlyVoiceMessagesToHDD() {
        int filesCounter = 0;
        for (TLDialog dialog : settings.dialogs) {

            System.out.println();
            System.out.println("Crawling dialog: " + ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), settings.chatsHashMap, settings.usersHashMap));
            System.out.println("Top message: " + dialog.getTopMessage());
            System.out.println();

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, settings.messagesHashMap);
            TLVector<TLAbsMessage> absMessages;
            absMessages = DialogsHistoryMethods.getWholeMessageHistory(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            System.out.println("Downloaded: " + absMessages.size());

            for (TLAbsMessage absMessage : absMessages)
                if (filesCounter < settings.FILES_LIMIT)
                    if (MediaDownloadMethods.messageDownloadVoiceMessagesToHDD(settings.api, absMessage, settings.MAX_FILE_SIZE, settings.FILES_PATH) != null)
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
     */
    public void saveOnlyMessagesToHDD() {
        for (TLDialog dialog : settings.dialogs) {

            String fullName = ConsoleOutputMethods.getDialogFullNameWithID(dialog.getPeer().getId(), settings.chatsHashMap, settings.usersHashMap);

            System.out.println();
            System.out.println("Crawling dialog: " + fullName);
            System.out.println("Top message: " + dialog.getTopMessage());

            //reads the messages
            TLAbsMessage topMessage = DialogsHistoryMethods.getTopMessage(dialog, settings.messagesHashMap);
            TLVector<TLAbsMessage> absMessages = DialogsHistoryMethods.getWholeMessageHistory(settings.api, dialog, settings.chatsHashMap, settings.usersHashMap, topMessage, settings.MESSAGES_LIMIT, settings.MAX_DATE, settings.MIN_DATE);
            System.out.println("Downloaded: " + absMessages.size());

            System.out.println("Writing: " + fullName + ".csv");
            // write here
            FileMethods.writeMessagesToCSV(absMessages, String.valueOf(dialog.getPeer().getId()), settings.FILES_PATH, ";");

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