/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.io;

import com.facebook.presto.common.NestedFieldIds;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.GroupField;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.VariantField;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.JSON;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.parquet.ParquetTypeUtils.getArrayElementColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByFieldId;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.parquet.io.ColumnIOUtil.columnDefinitionLevel;
import static org.apache.parquet.io.ColumnIOUtil.columnRepetitionLevel;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Placed in org.apache.parquet.io package to have access to ColumnIO getRepetitionLevel() and getDefinitionLevel() methods.
 */
public class ColumnIOConverter
{
    private ColumnIOConverter()
    {
    }

    public static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        return constructField(type, columnIO, Optional.empty());
    }

    /**
     * @param fieldIds the Iceberg field IDs of {@code type}'s nested fields, aligned with
     * the type's structure. When present, struct fields are matched against the file by
     * field ID (falling back to name only when a field has no ID), so a re-added or
     * renamed field reads as null rather than the stale column that reused its name.
     */
    public static Optional<Field> constructField(Type type, ColumnIO columnIO, Optional<NestedFieldIds> fieldIds)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnRepetitionLevel(columnIO);
        int definitionLevel = columnDefinitionLevel(columnIO);
        if (columnIO instanceof GroupColumnIO) {
            if (ROW.equals(type.getTypeSignature().getBase())) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                List<Type> parameters = type.getTypeParameters();
                ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
                List<TypeSignatureParameter> fields = type.getTypeSignature().getParameters();
                List<NestedFieldIds> fieldIdChildren = fieldIds.map(NestedFieldIds::getChildren).orElse(ImmutableList.of());
                checkArgument(fieldIdChildren.isEmpty() || fieldIdChildren.size() == fields.size(),
                        "field IDs do not align with the ROW type: expected %s children, got %s",
                        fields.size(), fieldIdChildren.size());
                boolean structHasParameters = false;
                for (int i = 0; i < fields.size(); i++) {
                    NamedTypeSignature namedTypeSignature = fields.get(i).getNamedTypeSignature();
                    String name = namedTypeSignature.getName().get().toLowerCase(Locale.ENGLISH);
                    NestedFieldIds fieldId = i < fieldIdChildren.size() ? fieldIdChildren.get(i) : null;
                    ColumnIO childColumnIO = lookupStructField(groupColumnIO, name, fieldId);
                    Optional<Field> field = constructField(parameters.get(i), childColumnIO, Optional.ofNullable(fieldId));
                    structHasParameters |= field.isPresent();
                    fieldsBuilder.add(field);
                }
                if (structHasParameters) {
                    return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
                }
                return Optional.empty();
            }
            else if (MAP.equals(type.getTypeSignature().getBase())) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                MapType mapType = (MapType) type;
                GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
                if (keyValueColumnIO.getChildrenCount() != 2) {
                    return Optional.empty();
                }
                Optional<NestedFieldIds> keyIds = fieldIds.map(ids -> ids.childAt(0));
                Optional<NestedFieldIds> valueIds = fieldIds.map(ids -> ids.childAt(1));
                Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0), keyIds);
                Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1), valueIds);
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
            }
            else if (ARRAY.equals(type.getTypeSignature().getBase())) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                List<Type> types = type.getTypeParameters();
                if (groupColumnIO.getChildrenCount() != 1) {
                    return Optional.empty();
                }
                Optional<NestedFieldIds> elementIds = fieldIds.map(ids -> ids.childAt(0));
                Optional<Field> field = constructField(types.get(0), getArrayElementColumn(groupColumnIO.getChild(0)), elementIds);
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
            }
            else if (JSON.equals(type.getTypeSignature().getBase())) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                // Look up by name: the physical column order is not guaranteed
                // (Iceberg / Spark write metadata first, value second)
                ColumnIO valueColumnIO = lookupColumnByName(groupColumnIO, "value");
                ColumnIO metadataColumnIO = lookupColumnByName(groupColumnIO, "metadata");
                // Shredded Variant files (Iceberg spec §4.3) carry a third child
                // "typed_value" alongside "metadata" and "value". We do not yet
                // support reconstructing values from typed_value, so reject them
                // explicitly rather than silently returning null for every row.
                boolean hasTypedValue = lookupColumnByName(groupColumnIO, "typed_value") != null;
                if (hasTypedValue) {
                    throw new IllegalArgumentException(
                            "Shredded Variant columns are not supported. " +
                                    "Column: " + groupColumnIO.getName());
                }
                if (valueColumnIO == null) {
                    throw new IllegalArgumentException("Value field is missing for variant type: " + type);
                }
                if (metadataColumnIO == null) {
                    throw new IllegalArgumentException("Metadata field is missing for variant type: " + type);
                }
                Optional<Field> value = constructField(VarbinaryType.VARBINARY, valueColumnIO);
                if (!value.isPresent()) {
                    throw new IllegalArgumentException("Value field is missing for variant type: " + type);
                }
                Optional<Field> metadata = constructField(VarbinaryType.VARBINARY, metadataColumnIO);
                if (!metadata.isPresent()) {
                    throw new IllegalArgumentException("Metadata field is missing for variant type: " + type);
                }
                return Optional.of(new VariantField(type, repetitionLevel, definitionLevel, required, value.get(), metadata.get()));
            }
        }
        else if (columnIO instanceof PrimitiveColumnIO) {
            PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
            RichColumnDescriptor column = new RichColumnDescriptor(primitiveColumnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
            return Optional.of(new PrimitiveField(type, repetitionLevel, definitionLevel, required, column, primitiveColumnIO.getId()));
        }
        return Optional.empty();
    }

    public static Optional<ColumnIO> findNestedColumnIO(ColumnIO columnIO, List<String> path)
    {
        return findNestedColumnIO(columnIO, path, ImmutableList.of());
    }

    /**
     * @param fieldIds the Iceberg field ID of each element of {@code path}, aligned
     * positionally. A path element is matched against the file by field ID when both are
     * known (falling back to name only when the file carries no field IDs), so a re-added
     * or renamed field reads as null rather than the stale column that reused its name.
     */
    public static Optional<ColumnIO> findNestedColumnIO(ColumnIO columnIO, List<String> path, List<Integer> fieldIds)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        checkArgument(fieldIds.size() <= path.size(), "field IDs do not align with the path: %s vs %s", fieldIds, path);

        for (int i = 0; i < path.size(); i++) {
            String pathElement = path.get(i);
            if (columnIO instanceof GroupColumnIO) {
                GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
                Integer fieldId = i < fieldIds.size() ? fieldIds.get(i) : null;
                columnIO = lookupStructField(groupColumnIO, pathElement, fieldId == null ? null : NestedFieldIds.leaf(OptionalInt.of(fieldId)));
                if (columnIO == null) {
                    return Optional.empty();
                }
                continue;
            }
            throw new IllegalArgumentException("Invalid ColumnIO received. Expected a GroupColumnIO, but got " + columnIO.getClass().getSimpleName() +
                    ", path=" + Joiner.on(".").join(path));
        }

        return Optional.of(columnIO);
    }

    /**
     * Finds the physical column of a struct field, preferring a match on Iceberg field ID
     * over a match on name. ID matching distinguishes a re-added or renamed field (new
     * ID) from the stale physical column (old ID) that reused its name in the file.
     * Files whose columns carry no field IDs at all (e.g. migrated tables) fall back to
     * name matching, as do fields with no known ID.
     */
    private static ColumnIO lookupStructField(GroupColumnIO groupColumnIO, String name, NestedFieldIds fieldId)
    {
        if (fieldId != null && fieldId.getId().isPresent() && hasFieldIds(groupColumnIO)) {
            ColumnIO columnIO = lookupColumnByFieldId(groupColumnIO, fieldId.getId().getAsInt());
            if (columnIO != null) {
                return columnIO;
            }
            // The file's columns carry field IDs but none matches this field's ID: the
            // field was added after the file was written. Return null (read as nulls)
            // rather than falling back to the name, which may hit the stale column of a
            // dropped field that reused the name.
            return null;
        }
        return lookupColumnByName(groupColumnIO, name);
    }

    private static boolean hasFieldIds(GroupColumnIO groupColumnIO)
    {
        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            if (groupColumnIO.getChild(i).getType().getId() != null) {
                return true;
            }
        }
        return false;
    }
}
