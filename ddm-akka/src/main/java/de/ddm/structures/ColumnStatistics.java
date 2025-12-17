package de.ddm.structures;

import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ColumnStatistics implements AkkaSerializable {
    private static final long serialVersionUID = 1756473829102837465L;

    private int attributeId;
    private int cardinality;
    private boolean hasNull;
    private String minValue;
    private String maxValue;
}
