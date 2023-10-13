import * as http from 'http';
import { internalAction } from '../_generated/server';
import { Id, TableNames } from '../_generated/dataModel';
const MILVUS_SERVICE_URL = 'https://3414c189y5.yicp.fun'; // Milvus 服务地址





// 检查Milvus服务是否已启动
export async function isMilvusServiceRunning(): Promise<boolean> {
    try {
        const response = await fetch(MILVUS_SERVICE_URL);
        return response.status === 200;
    } catch (error) {
        return false;
    }
}
if (!isMilvusServiceRunning()) {

    throw new Error(
        'Milvus Service is not Running!',
    );
}


export const deleteVectors = internalAction({
    handler: async (ctx, { tableName, ids }: { tableName: TableNames; ids: Id<TableNames>[] }) => {
        ;
    },
});


export const deleteAllVectors = internalAction({
    args: {},
    handler: async (ctx, args) => {
        if (await isMilvusServiceRunning()) {
            const response = await fetch(`${MILVUS_SERVICE_URL}/delete`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ deleteAll: true }),
            });

            const deletionResult = await response.json();
            return deletionResult;
        } else {
            return {};
        }
    },
});


export async function upsertVectors<TableName extends TableNames>(
    tableName: TableName,
    vectors: { id: Id<TableName>; values: number[]; metadata: object }[],
    index?: string,
) {
    // const start = Date.now();
    const results = [];
    // Insert all the vectors in batches of 100
    // https://docs.pinecone.io/docs/insert-data#batching-upserts
    for (let i = 0; i < vectors.length; i += 1) {
        try {
            const requestBody = {
                tableName: tableName,
                vector: vectors[i],
            };

            const response = await fetch(`${MILVUS_SERVICE_URL}/upsert`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestBody),
            });

            const data = await response.json();
            console.debug(data);

            results.push(data.message);
        } catch (error: any) {
            throw new Error(error.response?.data?.error || 'An error occurred while inserting vector');
        }
    }
    // console.debug(`Pinecone upserted ${vectors.length} vectors in ${Date.now() - start}ms`);
    return results;
}




// 查询相似向量
export async function queryVectors<TableName extends TableNames>(
    tableName: TableName,
    embedding: number[],
    filter: object,
    limit: number,
) {

    const requestData = {
        embedding: embedding,
        tableName: tableName,
        topK: limit,
        filter: filter
    };
    // 发送POST请求到Python服务
    const response = await fetch(`${MILVUS_SERVICE_URL}/query`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestData),
    });
    if (response.status === 200) {
        const result = await response.json();
        const parsedResult = result.map(({ id, score }: { id: string, score: number }) => ({ _id: id, score })) as { _id: Id<TableName>; score: number }[];
        return parsedResult;
    }

}


