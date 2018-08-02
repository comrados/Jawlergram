/*
 * Title: CrawlerMaps.java
 * Project: Jawlergram
 * Creator: Georgii Mikriukov
 * 2018
 */

package com.crawlergram.crawler.structures;

import org.telegram.api.chat.TLAbsChat;
import org.telegram.api.dialog.TLDialog;
import org.telegram.api.message.TLAbsMessage;
import org.telegram.api.user.TLAbsUser;
import org.telegram.tl.TLVector;

import java.util.HashMap;
import java.util.Map;

public class CrawlerMaps {
    public Map<Integer, TLAbsChat> chatsHashMap;
    public Map<Integer, TLAbsUser> usersHashMap;
    public TLVector<TLDialog> dialogs;
    public Map<Integer, TLAbsMessage> messagesHashMap;
    
    public CrawlerMaps(){
        Map<Integer, TLAbsChat> chatsHashMap = new HashMap<>();
        Map<Integer, TLAbsUser> usersHashMap = new HashMap<>();
        TLVector<TLDialog> dialogs = new TLVector<>();
        Map<Integer, TLAbsMessage> messagesHashMap = new HashMap<>();
    }
}
