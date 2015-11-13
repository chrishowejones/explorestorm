package com.devcycle.explorestorm.scheme;

import org.json.JSONObject;
import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.isA;

/**
 * Created by chrishowe-jones on 04/11/15.
 */
public class CBSMessageFieldsTest {

    @Test
    public void testGetTypeSeqNum() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(Long.class, cbsMessageFields.getType(CBSMessageFields.FIELD_SEQNUM));
    }

    @Test
    public void testGetTypeAccountNumber() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(Long.class, cbsMessageFields.getType(CBSMessageFields.FIELD_ACCOUNT_NUMBER));
    }

    @Test
    public void testGetTypeTime() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(String.class, cbsMessageFields.getType(CBSMessageFields.FIELD_TIME));
    }

    @Test
    public void testGetTypeTxnType() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(Integer.class, cbsMessageFields.getType(CBSMessageFields.FIELD_TXN_TYPE));
    }

    @Test
    public void testGetTypeTxnCode() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(Integer.class, cbsMessageFields.getType(CBSMessageFields.FIELD_TXN_CODE));
    }

    @Test
    public void testGetTypeTxnAmount() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(BigDecimal.class, cbsMessageFields.getType(CBSMessageFields.FIELD_TXN_AMOUNT));
    }

    @Test
    public void testGetCurrencyCode() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(Integer.class, cbsMessageFields.getType(CBSMessageFields.FIELD_CURRENCY_CDE));
    }

    @Test
    public void testGetTypeCurrentAccountBalance() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(BigDecimal.class, cbsMessageFields.getType(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE));
    }

    @Test
    public void testGetTypeCurrentDate() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(String.class, cbsMessageFields.getType(CBSMessageFields.FIELD_CURRENT_DATE));
    }

    @Test
    public void testGetTypeTxnDate() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(String.class, cbsMessageFields.getType(CBSMessageFields.FIELD_TXN_DATE));
    }

    @Test
    public void testGetTypeTxnNarrative() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(String.class, cbsMessageFields.getType(CBSMessageFields.FIELD_TXN_NARRATIVE));
    }

    @Test
    public void testGetTypeFullMessageClass() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(JSONObject.class, cbsMessageFields.getType(CBSMessageFields.FIELD_FULL_MESSAGE));
    }

    @Test
    public void testGetTypeTxnClass() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(Integer.class, cbsMessageFields.getType(CBSMessageFields.FIELD_TXN_CLASS));
    }

    @Test
    public void testGetTypeBranchCode() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(Integer.class, cbsMessageFields.getType(CBSMessageFields.FIELD_BRANCH_CODE));
    }

    @Test
    public void testGetTypeStem() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(Long.class, cbsMessageFields.getType(CBSMessageFields.FIELD_PARENT_STEM));
    }

    @Test
    public void testGetTypeMsgTimestamp() throws Exception {
        CBSMessageFields cbsMessageFields = new CBSMessageFields();
        assertEquals(Long.class, cbsMessageFields.getType(CBSMessageFields.FIELD_MSG_TIMESTAMP));
    }

}