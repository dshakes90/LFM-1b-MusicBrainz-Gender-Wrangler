package org.dshakes.musicbrainz;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datavec.api.transform.ColumnType;
import org.datavec.api.transform.Transform;
import org.datavec.api.transform.metadata.ColumnMetaData;
import org.datavec.api.transform.metadata.StringMetaData;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.writable.Text;
import org.datavec.api.writable.Writable;
import org.nd4j.shade.jackson.annotation.JsonIgnoreProperties;
import org.nd4j.shade.jackson.annotation.JsonInclude;
import org.nd4j.shade.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties({"inputSchema", "insertAfterIdx", "deriveFromIdx"})
@EqualsAndHashCode(exclude = {"inputSchema", "insertAfterIdx", "deriveFromIdx"})
@Data
public class DeriveGenderFromArtistNameTransform implements Transform {

    private final String columnName;
    private final String insertAfter;
    private final List<DerivedColumn> derivedColumns;
    private Schema inputSchema;
    private int insertAfterIdx = -1;
    private int deriveFromIdx = -1;

    private DeriveGenderFromArtistNameTransform(Builder builder) {
        this.derivedColumns = builder.derivedColumns;
        this.columnName = builder.columnName;
        this.insertAfter = builder.insertAfter;
    }

    @Override
    public Schema transform(Schema inputSchema) {
        List<ColumnMetaData> oldMeta = inputSchema.getColumnMetaData();
        List<ColumnMetaData> newMeta = new ArrayList<>(oldMeta.size() + derivedColumns.size());

        List<String> oldNames = inputSchema.getColumnNames();

        for (int i = 0; i < oldMeta.size(); i++) {
            String current = oldNames.get(i);
            newMeta.add(oldMeta.get(i));

            if (insertAfter.equals(current)) {
                //Insert the derived columns here
                for (DerivedColumn d : derivedColumns) {
                    switch (d.columnType) {
                        case String:
                            newMeta.add(new StringMetaData(d.columnName));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected column type: " + d.columnType);
                    }
                }
            }
        }

        return inputSchema.newSchema(newMeta);
    }

    @Override
    public void setInputSchema(Schema inputSchema) {
        insertAfterIdx = inputSchema.getColumnNames().indexOf(insertAfter);
        if (insertAfterIdx == -1) {
            throw new IllegalStateException(
                "Invalid schema/insert after column: input schema does not contain column \"" + insertAfter
                    + "\"");
        }

        deriveFromIdx = inputSchema.getColumnNames().indexOf(columnName);
        if (deriveFromIdx == -1) {
            throw new IllegalStateException(
                "Invalid source column: input schema does not contain column \"" + columnName + "\"");
        }

        this.inputSchema = inputSchema;

        if (!(inputSchema.getMetaData(columnName) instanceof StringMetaData))
            throw new IllegalStateException("Invalid state: input column \"" + columnName
                + "\" is not a time column. Is: " + inputSchema.getMetaData(columnName));
    }

    @Override
    public Schema getInputSchema() {
        return inputSchema;
    }

    @Override
    public List<Writable> map(List<Writable> writables) {
        if (writables.size() != inputSchema.numColumns()) {
            throw new IllegalStateException("Cannot execute transform: input writables list length (" + writables.size()
                + ") does not " + "match expected number of elements (schema: " + inputSchema.numColumns()
                + "). Transform = " + toString());
        }

        int i = 0;



        Writable mbidSource = writables.get(deriveFromIdx);
        Writable source = writables.get(deriveFromIdx-1);
        Writable idSource = writables.get(deriveFromIdx - 2);


        List<Writable> list = new ArrayList<>(writables.size() + derivedColumns.size());
        for (Writable w : writables) {
            list.add(w);
            if (i++ == insertAfterIdx) {
                for (DerivedColumn d : derivedColumns) {
                    switch (d.columnType) {
                        case String:
                            list.add(new Text(DeriveGenderFromDb.getArtistGender(source.toString(), idSource.toInt(), mbidSource.toString())));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected column type: " + d.columnType);
                    }
                }
            }
        }
        return list;
    }

    @Override
    public List<List<Writable>> mapSequence(List<List<Writable>> sequence) {
        List<List<Writable>> out = new ArrayList<>(sequence.size());
        for (List<Writable> step : sequence) {
            out.add(map(step));
        }
        return out;
    }

    /**
     * Transform an object
     * in to another object
     *
     * @param input the record to transform
     * @return the transformed writable
     */
    @Override
    public Object map(Object input) {
        List<Object> ret = new ArrayList<>();
        Long l = (Long) input;
        for (DerivedColumn d : derivedColumns) {
            switch (d.columnType) {
                case String:
                    //ret.add(d.dateTimeFormatter.print(l));
                    break;
                default:
                    throw new IllegalStateException("Unexpected column type: " + d.columnType);
            }
        }

        return ret;
    }

    /**
     * Transform a sequence
     *
     * @param sequence
     */
    @Override
    public Object mapSequence(Object sequence) {
        List<Long> longs = (List<Long>) sequence;
        List<List<Object>> ret = new ArrayList<>();
        for (Long l : longs)
            ret.add((List<Object>) map(l));
        return ret;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DeriveGenderFromArtistNameTransform(timeColumn=\"").append(columnName).append("\",insertAfter=\"")
            .append(insertAfter).append("\",derivedColumns=(");

        boolean first = true;
        for (DerivedColumn d : derivedColumns) {
            if (!first)
                sb.append(",");
            sb.append(d);
            first = false;
        }

        sb.append("))");

        return sb.toString();
    }

    /**
     * The output column name
     * after the operation has been applied
     *
     * @return the output column name
     */
    @Override
    public String outputColumnName() {
        return outputColumnNames()[0];
    }

    /**
     * The output column names
     * This will often be the same as the input
     *
     * @return the output column names
     */
    @Override
    public String[] outputColumnNames() {
        String[] ret = new String[derivedColumns.size()];
        for (int i = 0; i < ret.length; i++)
            ret[i] = derivedColumns.get(i).columnName;
        return ret;
    }

    /**
     * Returns column names
     * this op is meant to run on
     *
     * @return
     */
    @Override
    public String[] columnNames() {
        return new String[]{columnName()};
    }

    /**
     * Returns a singular column name
     * this op is meant to run on
     *
     * @return
     */
    @Override
    public String columnName() {
        return columnName;
    }

    public static class Builder {

        private final String columnName;
        private String insertAfter;
        private final List<DerivedColumn> derivedColumns = new ArrayList<>();


        /**
         * @param timeColumnName The name of the time column from which to derive the new values
         */
        public Builder(String timeColumnName) {
            this.columnName = timeColumnName;
            this.insertAfter = timeColumnName;
        }

        /**
         * Where should the new columns be inserted?
         * By default, they will be inserted after the source column
         *
         * @param columnName Name of the column to insert the derived columns after
         */
        public Builder insertAfter(String columnName) {
            this.insertAfter = columnName;
            return this;
        }

        public Builder addStringDerivedColumn(String columnName) {
            derivedColumns.add(new DerivedColumn(columnName, ColumnType.String));
            return this;
        }

        /**
         * Create the transform instance
         */
        public DeriveGenderFromArtistNameTransform build() {
            return new DeriveGenderFromArtistNameTransform(this);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @EqualsAndHashCode
    @Data
    @JsonIgnoreProperties({"dateTimeFormatter"})
    public static class DerivedColumn implements Serializable {
        private final String columnName;
        private final ColumnType columnType;

        public DerivedColumn(@JsonProperty("columnName") String columnName,
                             @JsonProperty("columnType") ColumnType columnType) {
            this.columnName = columnName;
            this.columnType = columnType;
        }

        @Override
        public String toString() {
            return "(name=" + columnName + ",type=" + columnType + ")";
        }

        //Custom serialization methods, because Joda Time doesn't allow DateTimeFormatter objects to be serialized :(
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
        }
    }
}
