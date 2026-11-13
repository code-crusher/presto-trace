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
package com.facebook.presto.common;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The Iceberg field IDs of a column's nested fields, aligned with the structure of the
 * Presto type it is read into: the children of a node correspond positionally to the
 * fields of a ROW, the single element of an ARRAY, or the key and value of a MAP.
 * A node's {@link #getId() id} is the Iceberg field ID of that node itself.
 *
 * <p>Readers use these IDs to match nested fields in a data file by Iceberg field ID
 * rather than by name, so that a field dropped and re-added (or renamed and re-added
 * under the old name) reads as null on rows written before the change, instead of
 * picking up the stale physical column that happens to share its name.
 *
 * <p>IDs are optional throughout: a file (or a sub-tree) written without field IDs
 * falls back to name-based matching.
 */
public final class NestedFieldIds
{
    private static final NestedFieldIds EMPTY = new NestedFieldIds(OptionalInt.empty(), ImmutableList.of());

    private final OptionalInt id;
    private final List<NestedFieldIds> children;

    private NestedFieldIds(OptionalInt id, List<NestedFieldIds> children)
    {
        this.id = requireNonNull(id, "id is null");
        this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
        checkArgument(children.isEmpty() || id.isPresent(), "a node with children must have an id");
    }

    /**
     * The node for a field with no known ID, or for a nested position this node does
     * not describe. An empty node carries no children, so lookups below it all fall
     * back to name-based matching.
     */
    public static NestedFieldIds empty()
    {
        return EMPTY;
    }

    public static NestedFieldIds leaf(OptionalInt id)
    {
        return new NestedFieldIds(id, ImmutableList.of());
    }

    public static NestedFieldIds node(int id, List<NestedFieldIds> children)
    {
        return new NestedFieldIds(OptionalInt.of(id), children);
    }

    /**
     * The Iceberg field ID of this node, empty when unknown (e.g. for the root, which
     * corresponds to a top-level column and is matched by the caller).
     */
    public OptionalInt getId()
    {
        return id;
    }

    /**
     * The IDs of this node's nested fields, aligned with the corresponding Presto type:
     * one per ROW field, a single element for ARRAY, or key and value for MAP. Empty for
     * primitive types.
     */
    public List<NestedFieldIds> getChildren()
    {
        return children;
    }

    /**
     * Returns the child at {@code index} (the ROW field, ARRAY element, or MAP key/value
     * at that position). When this node has no child there, returns {@link #empty()} so
     * that the sub-tree below it falls back to name-based matching.
     */
    public NestedFieldIds childAt(int index)
    {
        if (index >= children.size()) {
            return EMPTY;
        }
        return children.get(index);
    }

    @Override
    public String toString()
    {
        return id + " -> " + children;
    }
}
