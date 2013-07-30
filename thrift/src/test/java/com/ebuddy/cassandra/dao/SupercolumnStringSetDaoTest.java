package com.ebuddy.cassandra.dao;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.UUID;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.dao.mapper.ColumnMapper;

/**
 * Tests for SupercolumnStringSetDao.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class SupercolumnStringSetDaoTest {
    private SupercolumnStringSetDao<String,String> service;
    private final String rowKey = UUID.randomUUID().toString();

    @Mock
    private SuperColumnFamilyOperations<String,String,String,String> operations;
    @Mock
    private BatchContext batchContext;
    @Captor
    private ArgumentCaptor<ColumnMapper<String,String,String>> mapperCaptor;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(operations.begin()).thenReturn(batchContext);
        service = new SupercolumnStringSetDao<String,String>("email", "defaultEmail", operations);
    }

    @Test(groups = {"unit"})
    public void shouldWriteDefaultEmailAddress() throws Exception {
        service.writeElement(rowKey, "xms", "encrypted-email", true);
        verify(operations).writeColumn(rowKey, "xms", "email:encrypted-email", "", batchContext);
        verify(operations).writeColumn(rowKey,  "xms", "defaultEmail", "encrypted-email", batchContext);
        verify(operations).commit(batchContext);
    }

    @Test(groups = {"unit"})
    public void shouldWriteNondefaultEmailAddress() throws Exception {
        service.writeElement(rowKey, "xms", "encrypted-email", false);
        verify(operations).writeColumn(rowKey, "xms", "email:encrypted-email", "", batchContext);
        verify(operations, never()).writeColumn(rowKey,
                                                "xms",
                                                "defaultEmail",
                                                "encrypted-email", batchContext);
        verify(operations).commit(batchContext);
    }

    @Test(groups = {"unit"})
    public void shouldReadDefaultEmailAddress() throws Exception {
        service.readDefaultElement(rowKey, "xms");
        verify(operations).readColumnValue(rowKey, "xms", "defaultEmail");
    }

    @Test(groups = {"unit"})
    public void shouldReadEmailAddresses() throws Exception {
        service.readElements(rowKey, "xms");
        verify(operations).readColumns(eq(rowKey),
                                       eq("xms"),
                                       eq("email:"),
                                       eq("email:\uffff"),
                                       eq(Integer.MAX_VALUE),
                                       eq(false),
                                       mapperCaptor.capture());
        ColumnMapper <String,String,String> mapper = mapperCaptor.getValue();
        String email = mapper.mapColumn("email:encrypted-email", "");
        assertEquals(email, "encrypted-email");
    }
}
