package de.ddm.actors.profiling;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class DataProviderTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testStoreAndRequestColumn() {
        // Spawn DataProvider actor
        ActorRef<DataProvider.Message> dataProvider = testKit.spawn(DataProvider.create());

        // Create test data
        Set<String> testData = new HashSet<>();
        testData.add("value1");
        testData.add("value2");
        testData.add("value3");

        // Store column data
        dataProvider.tell(new DataProvider.StoreColumnMessage(1, testData));

        // Request column data
        TestProbe<DataProvider.ResponseColumnMessage> probe = testKit.createTestProbe();
        dataProvider.tell(new DataProvider.RequestColumnMessage(1, probe.getRef()));

        // Verify response
        DataProvider.ResponseColumnMessage response = probe.receiveMessage();
        assertEquals(1, response.getAttributeId());
        assertEquals(testData, response.getColumnData());
    }

    @Test
    public void testRequestNonExistentColumn() {
        // Spawn DataProvider actor
        ActorRef<DataProvider.Message> dataProvider = testKit.spawn(DataProvider.create());

        // Request non-existent column
        TestProbe<DataProvider.ResponseColumnMessage> probe = testKit.createTestProbe();
        dataProvider.tell(new DataProvider.RequestColumnMessage(999, probe.getRef()));

        // Verify response contains empty set
        DataProvider.ResponseColumnMessage response = probe.receiveMessage();
        assertEquals(999, response.getAttributeId());
        assertTrue(response.getColumnData().isEmpty());
    }

    @Test
    public void testRegisterAttributes() {
        // Spawn DataProvider actor
        ActorRef<DataProvider.Message> dataProvider = testKit.spawn(DataProvider.create());

        // Register attributes for worker
        int[] attributes = { 1, 2, 3, 4, 5 };
        dataProvider.tell(new DataProvider.SetIdAndAttributesMessage(1, attributes));

        // Query attribute owner
        TestProbe<DataProvider.ResponseAttributeOwnerMessage> probe = testKit.createTestProbe();
        dataProvider.tell(new DataProvider.QueryAttributeOwnerMessage(3, probe.getRef()));

        // Verify response
        DataProvider.ResponseAttributeOwnerMessage response = probe.receiveMessage();
        assertEquals(3, response.getAttributeId());
        assertEquals(1, response.getWorkerId());
    }

    @Test
    public void testQueryUnregisteredAttribute() {
        // Spawn DataProvider actor
        ActorRef<DataProvider.Message> dataProvider = testKit.spawn(DataProvider.create());

        // Query unregistered attribute
        TestProbe<DataProvider.ResponseAttributeOwnerMessage> probe = testKit.createTestProbe();
        dataProvider.tell(new DataProvider.QueryAttributeOwnerMessage(999, probe.getRef()));

        // Verify response indicates no owner
        DataProvider.ResponseAttributeOwnerMessage response = probe.receiveMessage();
        assertEquals(999, response.getAttributeId());
        assertEquals(-1, response.getWorkerId());
    }

    @Test
    public void testGetStatistics() {
        // Spawn DataProvider actor
        ActorRef<DataProvider.Message> dataProvider = testKit.spawn(DataProvider.create());

        // Store some data
        Set<String> data1 = new HashSet<>();
        data1.add("a");
        data1.add("b");
        dataProvider.tell(new DataProvider.StoreColumnMessage(1, data1));

        Set<String> data2 = new HashSet<>();
        data2.add("x");
        data2.add("y");
        data2.add("z");
        dataProvider.tell(new DataProvider.StoreColumnMessage(2, data2));

        // Get statistics
        TestProbe<DataProvider.StatisticsResponseMessage> probe = testKit.createTestProbe();
        dataProvider.tell(new DataProvider.GetStatisticsMessage(probe.getRef()));

        // Verify statistics
        DataProvider.StatisticsResponseMessage response = probe.receiveMessage();
        assertEquals(2, response.getTotalAttributes());
        assertEquals(5, response.getTotalValues()); // 2 + 3 values
        assertTrue(response.getMemoryUsageBytes() > 0);
    }
}
