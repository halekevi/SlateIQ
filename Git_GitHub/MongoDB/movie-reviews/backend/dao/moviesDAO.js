import mongodb from "mongodb";
const ObjectId = mongodb.ObjectId;

let moviesCollection;

export default class MoviesDAO {
    static async injectDB(conn) {
        if (moviesCollection) {
            return;
        }
        try {
            moviesCollection = await conn.db(process.env.MOVIEREVIEWS_NS).collection("movies");
        } catch (e) {
            console.error(`Unable to establish a collection handle in MoviesDAO: ${e}`);
        }
    }

    static async getMovies({
        filters = null,
        page = 0,
        moviesPerPage = 20,
    } = {}) {
        let query = {};
        if (filters) {
            if ("title" in filters) {
                query = { $text: { $search: filters['title'] } };
            } else if ("rated" in filters) {
                query = { "rated": { $eq: filters['rated'] } };
            }
        }

        let cursor;

        try {
            cursor = await moviesCollection
                .find(query)
                .limit(moviesPerPage)
                .skip(moviesPerPage * page);

            const moviesList = await cursor.toArray();
            const totalNumMovies = await moviesCollection.countDocuments(query);

            return { moviesList, totalNumMovies };
        } catch (e) {
            console.error(`Unable to issue find command, ${e}`);
            return { moviesList: [], totalNumMovies: 0 };
        }
    }
}
